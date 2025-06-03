import lmdb
import threading
import time
import asyncio
import msgpack
import logging
import random # Added for sampling
from cachetools import TTLCache
from typing import Optional, Any, Tuple, Callable

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CustomTTLCache(TTLCache):
    def __init__(self, maxsize, ttl, timer=time.monotonic, getsizeof=lambda _: 1): # Changed getsizeof
        super().__init__(maxsize, ttl, timer, getsizeof)
        self.eviction_callback = None

    def popitem(self):
        key, value = super().popitem()
        if self.eviction_callback:
            try:
                self.eviction_callback(key, value)
            except Exception as e: # Catch errors in callback
                logger.error(f"Error in TTLCache eviction_callback for key {key}: {e}", exc_info=True)
        return key, value

class Metrics:
    def __init__(self):
        self.lru_hits = 0
        self.lru_misses = 0
        self.lmdb_hits = 0
        self.lmdb_misses = 0
        self.lru_evictions = 0
        self.lmdb_deletions = 0
        self.lmdb_lru_evictions = 0
        self.get_latency = []
        self.put_latency = []

    def report(self):
        return {
            "lru_hits": self.lru_hits,
            "lru_misses": self.lru_misses,
            "lmdb_hits": self.lmdb_hits,
            "lmdb_misses": self.lmdb_misses,
            "lru_evictions": self.lru_evictions,
            "lmdb_deletions": self.lmdb_deletions,
            "lmdb_lru_evictions": self.lmdb_lru_evictions,
            "avg_get_latency_ms": (sum(self.get_latency) / len(self.get_latency) * 1000) if self.get_latency else 0,
            "avg_put_latency_ms": (sum(self.put_latency) / len(self.put_latency) * 1000) if self.put_latency else 0,
        }

class BaseLMDBCacheWrapper:
    def __init__(self, path: str,
                 lru_capacity: int = 1000,
                 lmdb_max_keys: Optional[int] = 1000000,
                 map_size: int = 10**10, # Made configurable
                 default_ttl: int = 3600,
                 cleanup_interval: int = 600,
                 lmdb_lru_db_name: bytes = b'lru_access_times',
                 lmdb_lru_sample_size: int = 5000):
        """
        Args:
            path: LMDB environment path.
            lru_capacity: Max items in in-memory TTLCache.
            lmdb_max_keys: Max keys allowed in LMDB (cold cache). Set to None or float('inf') for no limit.
            map_size: LMDB map size in bytes. This is a hard limit for the database's physical size.
            default_ttl: Default TTL in seconds for entries.
            cleanup_interval: Interval in seconds for background cleanup task.
            lmdb_lru_db_name: Name of the LMDB database for LRU access times.
            lmdb_lru_sample_size: Number of random entries to sample for LRU eviction.
        """
        self.env = lmdb.open(path, max_dbs=2, map_size=map_size, lock=True, max_readers=126, sync=True)
        self.db = self.env.open_db()
        self.lru_db = self.env.open_db(lmdb_lru_db_name)
        self.lru_cache = CustomTTLCache(maxsize=lru_capacity, ttl=default_ttl)
        # Convert lmdb_max_keys to float('inf') if None for easier comparison
        self.lmdb_max_keys = float('inf') if lmdb_max_keys is None else lmdb_max_keys
        self.default_ttl = default_ttl
        self.lock = threading.Lock() # Main lock for LMDB write operations and critical sections
        self.cleanup_interval = cleanup_interval
        self.lmdb_lru_sample_size = lmdb_lru_sample_size
        self.metrics = Metrics()

        # Hook eviction callback for metrics
        def on_lru_evict(key, value):
            self.metrics.lru_evictions += 1
            # logger.debug(f"TTLCache evicted key: {key}") # Optional: for detailed logging
        self.lru_cache.eviction_callback = on_lru_evict

    def _encode_key(self, key: str) -> bytes:
        return key.encode('utf-8')

    def _encode_value(self, value: Any, expire_at: int) -> bytes:
        # Using use_bin_type=True for more efficient binary data storage with msgpack
        packed_val = msgpack.packb({'expire_at': expire_at, 'value': value}, use_bin_type=True)
        return packed_val

    def _decode_value(self, data: bytes) -> Tuple[Optional[int], Any]:
        # Using raw=False to ensure strings are decoded to Python strings
        unpacked = msgpack.unpackb(data, raw=False)
        # Return expire_at as None if not present or 0, otherwise int
        expire_at = unpacked.get('expire_at')
        return expire_at if expire_at != 0 else None, unpacked.get('value')


    def _lmdb_get(self, key_b: bytes) -> Optional[bytes]:
        # Check if the instance is an AsyncLMDBCacheWrapper and if it's closing.
        # The hasattr is important because SyncLMDBCacheWrapper instances won't have _is_closing.
        # This method is called by both Async and Sync wrappers.
        is_async_and_closing = hasattr(self, '_is_closing') and self._is_closing

        if not self.env or is_async_and_closing:
            key_str = key_b.decode('utf-8', 'ignore')
            logger.warning(f"LMDB env closed or async cache closing, cannot get key {key_str}")
            return None
        with self.env.begin(db=self.db) as txn: # Not under self.lock for reads
            return txn.get(key_b)

    def _lmdb_put(self, key_b: bytes, val_b: bytes):
        # This lock ensures that LMDB writes and LRU eviction checks are atomic
        with self.lock:
            is_async_and_closing = hasattr(self, '_is_closing') and self._is_closing
            if not self.env or is_async_and_closing:
                logger.warning(f"LMDB env closed or async cache closing, cannot put key {key_b.decode('utf-8', 'ignore')}")
                return

            try:
                with self.env.begin(db=self.db, write=True) as wtxn:
                    current_entries = wtxn.stat()['entries']
                    # Check if we need to perform LRU eviction before this put
                    if self.lmdb_max_keys != float('inf') and current_entries >= self.lmdb_max_keys:
                        logger.warning(f"LMDB max keys limit {self.lmdb_max_keys} reached (current: {current_entries}). Initiating LRU eviction.")
                        self._lmdb_lru_evict(wtxn) # Pass the write transaction
                        current_entries = wtxn.stat()['entries'] # Re-check after eviction
                        if current_entries >= self.lmdb_max_keys:
                            # If still at/over limit, something went wrong or not enough was evicted
                            logger.error(f"LMDB max keys limit {self.lmdb_max_keys} still reached after LRU eviction (current: {current_entries}). Put operation for key {key_b.decode('utf-8', 'ignore')} will likely fail or increase size further.")
                            # Depending on strictness, could raise an error here.
                            # For now, proceeding with the put, but LMDB might raise MapFullError.

                    wtxn.put(key_b, val_b)

            except lmdb.MapFullError as e:
                logger.error(f"LMDB MapFullError during put for key {key_b.decode('utf-8', 'ignore')}: {e}. This may happen if map_size is too small or LRU eviction isn't aggressive enough.", exc_info=True)
                raise
            except lmdb.BadValsizeError as e: # Value too large for LMDB
                logger.error(f"LMDB BadValsizeError during put for key {key_b.decode('utf-8', 'ignore')}: {e}. Value might be too large.", exc_info=True)
                raise
            except lmdb.Error as e: # Other LMDB errors
                logger.error(f"LMDB general error during put for key {key_b.decode('utf-8', 'ignore')}: {e}", exc_info=True)
                raise


    def _lmdb_delete(self, key_b: bytes):
        with self.lock: # Ensure atomicity for deleting from both DBs
            is_async_and_closing = hasattr(self, '_is_closing') and self._is_closing
            if not self.env or is_async_and_closing:
                logger.warning(f"LMDB env closed or async cache closing, cannot delete key {key_b.decode('utf-8', 'ignore')}")
                return
            with self.env.begin(db=self.db, write=True) as main_txn:
                main_txn.delete(key_b)
            with self.env.begin(db=self.lru_db, write=True) as lru_txn:
                lru_txn.delete(key_b)
            # logger.debug(f"Deleted key {key_b.decode('utf-8','ignore')} from LMDB and LRU DB.")

    def _update_lmdb_access_time(self, key_b: bytes):
        # This lock is important if multiple threads/tasks could update access times concurrently.
        with self.lock:
            is_async_and_closing = hasattr(self, '_is_closing') and self._is_closing
            if not self.env or is_async_and_closing:
                logger.info(f"LMDB env closed or async cache closing, skipping access time update for {key_b.decode('utf-8', 'ignore')}")
                return
            with self.env.begin(db=self.lru_db, write=True) as txn:
                txn.put(key_b, str(time.time()).encode('utf-8'))

    def _lmdb_lru_evict(self, main_wtxn: lmdb.Transaction):
        """
        Evicts entries from LMDB using a sampling LRU approach if lmdb_max_keys is reached.
        This method is called from within _lmdb_put, already holding self.lock and within main_wtxn.
        """
        evicted_count = 0
        keys_to_evict_from_sample = []

        current_entries = main_wtxn.stat()['entries'] # Already in a transaction for self.db
        # Calculate how many entries to try to evict. Aim to go below, e.g., 90% of max_keys.
        # Or, evict a fixed percentage, or a number based on how much we are over.
        # For this version, let's aim to evict enough to go down to 90% of max_keys.
        target_eviction_count = current_entries - int(self.lmdb_max_keys * 0.90)

        if target_eviction_count <= 0:
            logger.info(f"LMDB LRU Eviction: No entries targeted for eviction (current: {current_entries}, max: {self.lmdb_max_keys}).")
            return

        logger.info(f"LMDB LRU Eviction: Targeting {target_eviction_count} evictions from a sample of {self.lmdb_lru_sample_size}.")

        sample_access_times = []
        # Read access times from lru_db. This needs its own transaction.
        with self.env.begin(db=self.lru_db) as lru_txn:
            cursor = lru_txn.cursor()
            # Iterate over a sample of keys. A truly random sample is hard with LMDB cursor.
            # Taking first N as a pseudo-random sample if keys are well-distributed.
            # Or, could try random seeks if keys are known/guessable, but that's complex.
            # For now, iterating and stopping at sample_size.
            for key, access_time_b in cursor:
                if len(sample_access_times) >= self.lmdb_lru_sample_size:
                    break
                try:
                    access_time = float(access_time_b.decode('utf-8'))
                    sample_access_times.append({'key': key, 'time': access_time})
                except ValueError:
                    logger.warning(f"Could not decode access time for key: {key.decode('utf-8', errors='ignore')} in lru_db. Skipping.")
                    # Optionally, delete such corrupted entries from lru_db
                    # with self.env.begin(db=self.lru_db, write=True) as lru_w_txn_corr:
                    # lru_w_txn_corr.delete(key)
                    continue

        if not sample_access_times:
            logger.info("LMDB LRU Eviction: No samples found in lru_db to evict from.")
            return

        # Sort samples by access time (oldest first)
        sample_access_times.sort(key=lambda x: x['time'])

        # Select keys to evict from the sorted sample, up to target_eviction_count
        keys_to_evict_from_sample = [item['key'] for item in sample_access_times[:target_eviction_count]]

        if keys_to_evict_from_sample:
            # Need a separate write transaction for lru_db deletions
            with self.env.begin(db=self.lru_db, write=True) as lru_wtxn_evict:
                for key_b in keys_to_evict_from_sample:
                    # Delete from main DB using the passed transaction (main_wtxn)
                    if main_wtxn.delete(key_b): # delete returns True if key existed
                        # Delete from LRU DB using its own transaction
                        lru_wtxn_evict.delete(key_b)
                        evicted_count += 1
                    else:
                        # Key might have been deleted by another process/ttl expiry between sampling and now
                        logger.debug(f"LMDB LRU Eviction: Key {key_b.decode('utf-8','ignore')} not found in main_db during eviction, might have been deleted elsewhere.")
                        lru_wtxn_evict.delete(key_b) # Still try to clean up from lru_db

            self.metrics.lmdb_lru_evictions += evicted_count
            logger.info(f"LMDB LRU Eviction: Successfully deleted {evicted_count} entries based on sample.")
        else:
            logger.info("LMDB LRU Eviction: No entries selected for eviction from the sample.")


    def _delete_expired_sync(self, batch_size: int = 1000) -> int:
        now = time.time()
        deleted_count = 0
        keys_for_batch_main_db_delete = []
        keys_for_batch_lru_db_delete = []

        with self.lock: # This lock ensures consistency during the cleanup
            is_async_and_closing = hasattr(self, '_is_closing') and self._is_closing
            if not self.env or is_async_and_closing:
                logger.warning("LMDB env closed or async cache closing, cannot run expired cleanup sync.")
                return 0

            # Iterate through the main DB to find expired items
            with self.env.begin(db=self.db) as main_txn_read:
                cursor = main_txn_read.cursor()
                for key_b, data in cursor:
                    # Re-check env status frequently if not holding lock for the whole loop
                    # but here we hold the lock for the whole _delete_expired_sync
                    try:
                        expire_at, _ = self._decode_value(data)
                        # expire_at can be None if no TTL was set.
                        if expire_at is not None and expire_at < now:
                            keys_for_batch_main_db_delete.append(key_b)
                            keys_for_batch_lru_db_delete.append(key_b)

                            key_str = key_b.decode('utf-8', errors='ignore')
                            if key_str in self.lru_cache: # Check before pop to avoid KeyError
                                self.lru_cache.pop(key_str, None)

                            if len(keys_for_batch_main_db_delete) >= batch_size:
                                break # Limit batch size for this iteration
                    except msgpack.UnpackException:
                        logger.warning(f"Corrupted data found in main_db for key {key_b.decode('utf-8','ignore')} during expired check. Marking for deletion.")
                        keys_for_batch_main_db_delete.append(key_b)
                        keys_for_batch_lru_db_delete.append(key_b)
                    # Add more specific error handling if needed

            # Perform batch deletions if any keys were collected
            if keys_for_batch_main_db_delete:
                with self.env.begin(db=self.db, write=True) as main_txn_write:
                    for key_b in keys_for_batch_main_db_delete:
                        if main_txn_write.delete(key_b): # delete returns True if key existed
                            deleted_count += 1

                with self.env.begin(db=self.lru_db, write=True) as lru_txn_write:
                    for key_b in keys_for_batch_lru_db_delete:
                        lru_txn_write.delete(key_b) # Attempt to delete from lru_db as well

        if deleted_count > 0:
            self.metrics.lmdb_deletions += deleted_count
            logger.info(f"[Sync Expired Cleanup] Deleted {deleted_count} expired entries from LMDB.")
        return deleted_count

    def close(self):
        # This method is now designed to be called when self.lock is already held.
        if hasattr(self, 'env') and self.env:
            logger.info(f"Closing LMDB environment for {type(self).__name__}...")
            self.env.close()
            self.env = None # Mark as closed to prevent further use
            logger.info(f"LMDB environment closed for {type(self).__name__}.")
        else:
            logger.info(f"LMDB environment for {type(self).__name__} was already closed or not initialized.")


class AsyncLMDBCacheWrapper(BaseLMDBCacheWrapper):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._cleanup_task: Optional[asyncio.Task] = None
        self._stop_cleanup = asyncio.Event()
        self._is_closing = False # Flag to signal closing

    async def get(self, key: str) -> Optional[Any]:
        start_time = time.monotonic()

        if self._is_closing:
            logger.warning(f"Cache is closing. Get for key '{key}' (from TTLCache or None).")
            # Try to serve from TTLCache even if closing, as it's in-memory.
            if key in self.lru_cache:
                self.metrics.lru_hits += 1
                self.metrics.get_latency.append(time.monotonic() - start_time)
                return self.lru_cache[key]
            self.metrics.get_latency.append(time.monotonic() - start_time)
            return None  # Not in TTLCache, and won't check LMDB.

        # Check TTLCache first
        if key in self.lru_cache:
            val = self.lru_cache[key]
            self.metrics.lru_hits += 1
            # Update LMDB access time, but only if not closing.
            if not self._is_closing: # Re-check, as state might change during await for lock in other tasks
                asyncio.create_task(asyncio.to_thread(self._update_lmdb_access_time, self._encode_key(key)))
            self.metrics.get_latency.append(time.monotonic() - start_time)
            return val

        self.metrics.lru_misses += 1
        key_b = self._encode_key(key)

        # Offload LMDB get operation to a thread. _lmdb_get itself now checks for closing/env.
        data = await asyncio.to_thread(self._lmdb_get, key_b)

        if data is None: # True if key not found, or if _lmdb_get aborted due to closing/env issue.
            self.metrics.lmdb_misses += 1
            self.metrics.get_latency.append(time.monotonic() - start_time)
            return None

        try:
            expire_at, value = self._decode_value(data)
        except msgpack.UnpackException as e:
            logger.error(f"Failed to decode value for key '{key}' from LMDB: {e}", exc_info=True)
            if not self._is_closing:
                asyncio.create_task(asyncio.to_thread(self._lmdb_delete, key_b))
            self.metrics.lmdb_deletions += 1
            self.metrics.get_latency.append(time.monotonic() - start_time)
            return None

        current_time = time.time()
        if expire_at is not None and current_time > expire_at:
            logger.info(f"Key '{key}' found in LMDB but expired. Deleting.")
            if not self._is_closing:
                asyncio.create_task(asyncio.to_thread(self._lmdb_delete, key_b))
            self.metrics.lmdb_deletions += 1
            self.metrics.get_latency.append(time.monotonic() - start_time)
            return None

        self.metrics.lmdb_hits += 1
        if not self._is_closing: # Re-check before task creation
            asyncio.create_task(asyncio.to_thread(self._update_lmdb_access_time, key_b))
        try:
            self.lru_cache[key] = value
        except ValueError: # e.g. if TTLCache is full and can't evict (shouldn't happen with maxsize > 0)
             logger.warning(f"Could not add key '{key}' to TTLCache, it might be full or an issue with the value.", exc_info=True)

        self.metrics.get_latency.append(time.monotonic() - start_time)
        return value

    async def put(self, key: str, value: Any, ttl: Optional[int] = None):
        start_time = time.monotonic()
        if self._is_closing:
            logger.warning(f"Cache is closing. Put for key '{key}' rejected.")
            self.metrics.put_latency.append(time.monotonic() - start_time)
            return

        actual_ttl = ttl if ttl is not None else self.default_ttl
        expire_at_timestamp = int(time.time() + actual_ttl) if actual_ttl > 0 else 0

        key_b = self._encode_key(key)
        val_b = self._encode_value(value, expire_at_timestamp)

        try:
            # _lmdb_put is now fortified with _is_closing check
            await asyncio.to_thread(self._lmdb_put, key_b, val_b)

            # Check if _lmdb_put did work or returned early due to closing state
            # This requires _lmdb_put to signal if it did work, e.g. by returning a status.
            # For now, we assume if no error, it might have worked or correctly aborted.
            # The main effect of _is_closing in _lmdb_put is to prevent LMDB ops.

            if not self._is_closing: # Check again before task creation
                asyncio.create_task(asyncio.to_thread(self._update_lmdb_access_time, key_b))

            # Add to TTLCache only if not closing to prevent it from being immediately useless if cache is cleared on close.
            # Or, allow it, as TTLCache is in-memory and fast. Let's allow it for now.
            self.lru_cache[key] = value

        except lmdb.MapFullError:
            logger.error(f"LMDB MapFullError on async put for key '{key}'. Cache is full.", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"Error during async put operation for key '{key}': {e}", exc_info=True)
            raise

        self.metrics.put_latency.append(time.monotonic() - start_time)

    async def delete(self, key: str) -> None:
        if self._is_closing:
            logger.warning(f"Cache is closing. Delete for key '{key}' rejected.")
            return

        key_b = self._encode_key(key)
        # _lmdb_delete is now fortified
        await asyncio.to_thread(self._lmdb_delete, key_b)
        self.lru_cache.pop(key, None)
        self.metrics.lmdb_deletions += 1
        logger.info(f"Async deleted key '{key}' from cache.")


    async def delete_old_entries(self, batch_size: int = 1000) -> int:
        if self._is_closing or not self.env: # Initial check
            logger.info("Cache is closing or LMDB env not available, delete_old_entries skipped.")
            return 0
        # _delete_expired_sync is now fortified
        deleted_count = await asyncio.to_thread(self._delete_expired_sync, batch_size)
        return deleted_count

    async def start_background_cleanup(self):
        if self._is_closing or not self.env: # Initial check
            logger.warning("Cannot start background cleanup: Cache is closing or LMDB env not available.")
            return
        if self._cleanup_task is None or self._cleanup_task.done():
            self._stop_cleanup.clear()
            self._cleanup_task = asyncio.create_task(self._cleanup_loop())
            logger.info("Async cache background cleanup task initiated.")
        else:
            logger.info("Async cache background cleanup task is already running.")

    async def stop_background_cleanup(self):
        if self._cleanup_task and not self._cleanup_task.done():
            logger.info("Stopping async cache background cleanup task...")
            self._stop_cleanup.set()
            try:
                # Wait for the task to finish, with a timeout slightly longer than the interval
                await asyncio.wait_for(self._cleanup_task, timeout=self.cleanup_interval + 10)
            except asyncio.TimeoutError:
                logger.warning("Timeout waiting for background cleanup task to stop. Attempting cancellation.")
                self._cleanup_task.cancel()
                try:
                    await self._cleanup_task # Await cancellation
                except asyncio.CancelledError:
                    logger.info("Background cleanup task was cancelled.")
            except Exception as e:
                logger.error(f"Error encountered while stopping background cleanup task: {e}", exc_info=True)
            self._cleanup_task = None
            logger.info("Async cache background cleanup task stopped.")
        else:
            logger.info("Async cache background cleanup task was not running or already stopped.")


    async def _cleanup_loop(self):
        logger.info(f"Async cleanup loop started. Interval: {self.cleanup_interval}s.")
        while not self._stop_cleanup.is_set():
            if self._is_closing or not self.env: # Primary check for loop continuation
                logger.info("Async cleanup loop: Cache is closing or LMDB env not available. Exiting.")
                break
            try:
                logger.debug("Async cleanup loop: Checking for expired entries...")
                # delete_old_entries has its own _is_closing/env checks
                deleted_count = await self.delete_old_entries()
                if deleted_count > 0:
                    logger.info(f"[Async Cleanup] Cycle complete. Deleted {deleted_count} expired entries.")
                else:
                    logger.debug("[Async Cleanup] Cycle complete. No expired entries found.")
            except Exception as e:
                logger.error(f"[Async Cleanup] Error during cleanup cycle: {e}", exc_info=True)

            try:
                # Wait for the cleanup interval or until stop is signaled
                # This makes the loop wake up every `cleanup_interval` seconds
                await asyncio.wait_for(self._stop_cleanup.wait(), timeout=self.cleanup_interval)
                # If wait() returns, it means _stop_cleanup was set.
                if self._stop_cleanup.is_set():
                    logger.info("Async cleanup loop: Stop event received, exiting.")
                    break
            except asyncio.TimeoutError:
                # This is the normal path for periodic execution.
                logger.debug("Async cleanup loop: Interval ended, proceeding to next cycle.")
                continue
            except Exception as e:
                logger.error(f"[Async Cleanup] Error in wait logic: {e}", exc_info=True)
                break # Exit loop on unexpected error in wait logic
        logger.info("Async cleanup loop finished.")


    async def close(self):
        logger.info("Closing AsyncLMDBCacheWrapper...")
        await self.stop_background_cleanup()
        self._is_closing = True # Signal that we are shutting down
        logger.info("AsyncLMDBCacheWrapper: _is_closing set to True. Stopping background cleanup...")
        await self.stop_background_cleanup()

        # Allow some time for in-flight operations to notice the _is_closing flag or complete
        await asyncio.sleep(0.1)

        def _locked_close_call():
            logger.info("AsyncLMDBCacheWrapper: Attempting to acquire lock for final close...")
            with self.lock: # Acquire the main lock
                logger.info("AsyncLMDBCacheWrapper: Lock acquired. Calling super().close().")
                super(AsyncLMDBCacheWrapper, self).close() # Corrected super() call

        if hasattr(self, 'env') and self.env: # Check if env seems open before trying locked close
            await asyncio.to_thread(_locked_close_call)
        else:
            logger.info("AsyncLMDBCacheWrapper: LMDB environment already None or closed before attempting locked close.")

        logger.info("AsyncLMDBCacheWrapper closed.")

class SyncLMDBCacheWrapper(BaseLMDBCacheWrapper):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._cleanup_thread: Optional[threading.Thread] = None
        self._stop_cleanup_event = threading.Event() # Used to signal the cleanup thread to stop

    def close(self): # Override close for SyncLMDBCacheWrapper
        logger.info("Closing SyncLMDBCacheWrapper...")
        self.stop_background_cleanup() # Signal and join its cleanup thread

        # Acquire lock and call base close method (which is now also lock-protected,
        # leading to a reentrant lock acquisition if the same lock instance is used, or separate if overridden)
        # BaseLMDBCacheWrapper.close now expects the lock to be held.
        logger.info("SyncLMDBCacheWrapper: Attempting to acquire lock for final close...")
        with self.lock:
            logger.info("SyncLMDBCacheWrapper: Lock acquired. Calling super().close().")
            super().close()
        logger.info("SyncLMDBCacheWrapper closed.")

    def get(self, key: str) -> Optional[Any]:
        start_time = time.monotonic()

        if key in self.lru_cache:
            val = self.lru_cache[key]
            self.metrics.lru_hits += 1
            self._update_lmdb_access_time(self._encode_key(key)) # Direct call
            self.metrics.get_latency.append(time.monotonic() - start_time)
            return val

        self.metrics.lru_misses += 1
        key_b = self._encode_key(key)
        data = self._lmdb_get(key_b) # Direct call

        if data is None:
            self.metrics.lmdb_misses += 1
            self.metrics.get_latency.append(time.monotonic() - start_time)
            return None

        try:
            expire_at, value = self._decode_value(data)
        except msgpack.UnpackException as e:
            logger.error(f"Failed to decode value for key '{key}' (sync): {e}", exc_info=True)
            self._lmdb_delete(key_b) # Direct call to delete malformed entry
            self.metrics.lmdb_deletions +=1
            self.metrics.get_latency.append(time.monotonic() - start_time)
            return None

        current_time = time.time()
        if expire_at is not None and current_time > expire_at:
            logger.info(f"Key '{key}' found in LMDB but expired (sync). Deleting.")
            self._lmdb_delete(key_b) # Direct call
            self.metrics.lmdb_deletions += 1
            self.lru_cache.pop(key, None)
            self.metrics.get_latency.append(time.monotonic() - start_time)
            return None

        self.metrics.lmdb_hits += 1
        self._update_lmdb_access_time(key_b) # Direct call
        try:
            self.lru_cache[key] = value
        except ValueError:
             logger.warning(f"Could not add key '{key}' to TTLCache (sync).", exc_info=True)
        self.metrics.get_latency.append(time.monotonic() - start_time)
        return value

    def put(self, key: str, value: Any, ttl: Optional[int] = None):
        start_time = time.monotonic()
        actual_ttl = ttl if ttl is not None else self.default_ttl
        expire_at_timestamp = int(time.time() + actual_ttl) if actual_ttl > 0 else 0

        key_b = self._encode_key(key)
        val_b = self._encode_value(value, expire_at_timestamp)

        try:
            self._lmdb_put(key_b, val_b) # Direct call
            self._update_lmdb_access_time(key_b)
            self.lru_cache[key] = value
        except lmdb.MapFullError:
            logger.error(f"LMDB MapFullError on sync put for key '{key}'. Cache is full.", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"Error during sync put operation for key '{key}': {e}", exc_info=True)
            raise

        self.metrics.put_latency.append(time.monotonic() - start_time)

    def delete(self, key: str) -> None:
        key_b = self._encode_key(key)
        self._lmdb_delete(key_b) # Direct call
        self.lru_cache.pop(key, None)
        self.metrics.lmdb_deletions += 1
        logger.info(f"Sync deleted key '{key}' from cache.")

    def start_background_cleanup(self):
        if not self.env:
            logger.warning("Cannot start sync background cleanup: LMDB environment not available.")
            return
        if self._cleanup_thread is None or not self._cleanup_thread.is_alive():
            self._stop_cleanup_event.clear()
            self._cleanup_thread = threading.Thread(target=self._cleanup_loop_sync, daemon=True)
            self._cleanup_thread.name = "SyncLMDBCacheCleanupThread"
            self._cleanup_thread.start()
            logger.info("Sync cache background cleanup thread started.")
        else:
            logger.info("Sync cache background cleanup thread is already running.")

    def stop_background_cleanup(self):
        if self._cleanup_thread and self._cleanup_thread.is_alive():
            logger.info("Stopping sync cache background cleanup thread...")
            self._stop_cleanup_event.set()
            # Wait for the thread to finish, with a timeout
            self._cleanup_thread.join(timeout=self.cleanup_interval + 10)
            if self._cleanup_thread.is_alive():
                 logger.warning("Timeout waiting for sync background cleanup thread to stop. It may take longer to exit.")
            else:
                logger.info("Sync cache background cleanup thread stopped.")
            self._cleanup_thread = None
        else:
            logger.info("Sync cache background cleanup thread was not running or already stopped.")

    def _cleanup_loop_sync(self):
        logger.info(f"Sync cleanup loop started. Interval: {self.cleanup_interval}s.")
        while not self._stop_cleanup_event.is_set():
            if not self.env: # Check if env was closed
                logger.warning("LMDB environment closed. Stopping sync cleanup loop.")
                break
            try:
                logger.debug("Sync cleanup loop: Checking for expired entries...")
                # Use the synchronous _delete_expired_sync from the base class
                deleted_count = self._delete_expired_sync()
                if deleted_count > 0:
                    logger.info(f"[Sync Cleanup] Cycle complete. Deleted {deleted_count} expired entries.")
                else:
                    logger.debug("[Sync Cleanup] Cycle complete. No expired entries found.")
            except Exception as e:
                logger.error(f"[Sync Cleanup] Error during cleanup cycle: {e}", exc_info=True)

            # Wait for the interval or until stop is signaled
            # wait returns true if the event was set (meaning stop), false if it timed out (normal interval)
            if self._stop_cleanup_event.wait(timeout=self.cleanup_interval):
                logger.info("Sync cleanup loop: Stop event received, exiting.")
                break # Event was set, so exit loop
            logger.debug("Sync cleanup loop: Interval ended, proceeding to next cycle.")
        logger.info("Sync cleanup loop finished.")

    def close(self):
        logger.info("Closing SyncLMDBCacheWrapper...")
        self.stop_background_cleanup()
        super().close() # Call base class close method
        logger.info("SyncLMDBCacheWrapper closed.")
