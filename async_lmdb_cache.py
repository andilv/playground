import asyncio
import lmdb
import logging
import pickle
import time
import random
from typing import Any, Optional, Tuple

logger = logging.getLogger(__name__)

class Metrics:
    """A simple class to track cache metrics."""

    def __init__(self) -> None:
        self.hits = 0
        self.misses = 0
        self.errors = 0
        self.latencies = []

    def record_hit(self, latency: float) -> None:
        """Records a cache hit."""
        self.hits += 1
        self.latencies.append(latency)

    def record_miss(self) -> None:
        """Records a cache miss."""
        self.misses += 1

    def record_error(self) -> None:
        """Records an error."""
        self.errors += 1

    def get_metrics(self) -> dict:
        """Returns a dictionary of current metrics."""
        return {
            "hits": self.hits,
            "misses": self.misses,
            "errors": self.errors,
            "avg_latency": sum(self.latencies) / len(self.latencies) if self.latencies else 0,
        }

class AsyncLMDBCacheWrapper:
    """A thread-safe asynchronous wrapper around an LMDB database for caching."""

    def __init__(self, path: str, map_size: int = 1024 * 1024 * 1024, lock_timeout: int = 5) -> None:
        """
        Initializes the LMDB cache.

        Args:
            path: Path to the LMDB database file.
            map_size: Maximum size of the LMDB database in bytes.
            lock_timeout: Timeout in seconds for acquiring a lock.
        """
        self.path = path
        self.map_size = map_size
        self.lock_timeout = lock_timeout
        self._env = None
        self._metrics = Metrics()
        self._init_db()

    def _init_db(self) -> None:
        """Initializes the LMDB environment."""
        try:
            self._env = lmdb.open(self.path, map_size=self.map_size, writemap=True, map_async=True, metasync=False, sync=False)
        except lmdb.Error as e:
            logger.error(f"Error initializing LMDB: {e}")
            self._metrics.record_error()
            raise

    async def get(self, key: str) -> Optional[Any]:
        """
        Retrieves an item from the cache.

        Args:
            key: The key of the item to retrieve.

        Returns:
            The cached item, or None if the item is not found or an error occurs.
        """
        start_time = time.monotonic()
        loop = asyncio.get_event_loop()
        try:
            with self._env.begin() as txn:
                value = await loop.run_in_executor(None, txn.get, key.encode())
            if value:
                self._metrics.record_hit(time.monotonic() - start_time)
                # Deserialize using pickle
                return pickle.loads(value)
            else:
                self._metrics.record_miss()
                return None
        except lmdb.Error as e:
            logger.error(f"Error getting item from LMDB: {e}")
            self._metrics.record_error()
            return None
        except pickle.PickleError as e:
            logger.error(f"Error deserializing item from LMDB: {e}")
            self._metrics.record_error()
            # Potentially delete the corrupted entry
            await self.delete(key)
            return None


    async def put(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """
        Puts an item into the cache.

        Args:
            key: The key of the item to put.
            value: The item to put.
            ttl: Optional time-to-live in seconds.
        """
        loop = asyncio.get_event_loop()
        try:
            # Serialize using pickle
            serialized_value = pickle.dumps(value)
            if ttl:
                # Store expiry time along with the value
                expire_at = time.time() + ttl
                data_to_store = pickle.dumps((serialized_value, expire_at))
            else:
                data_to_store = pickle.dumps((serialized_value, None))

            with self._env.begin(write=True) as txn:
                await loop.run_in_executor(None, txn.put, key.encode(), data_to_store)
        except lmdb.MapFullError:
            logger.warning("LMDB map is full. Attempting to delete old entries.")
            self._metrics.record_error()
            await self.delete_old_entries(ratio_to_delete=0.1) # Delete 10% of entries
            # Retry putting the item after cleanup
            try:
                with self._env.begin(write=True) as txn:
                    await loop.run_in_executor(None, txn.put, key.encode(), data_to_store)
            except lmdb.Error as e: # Catch potential errors during retry
                logger.error(f"Error putting item into LMDB after cleanup: {e}")
                self._metrics.record_error()
        except (lmdb.Error, pickle.PickleError) as e:
            logger.error(f"Error putting item into LMDB: {e}")
            self._metrics.record_error()

    async def delete(self, key: str) -> None:
        """
        Deletes an item from the cache.

        Args:
            key: The key of the item to delete.
        """
        loop = asyncio.get_event_loop()
        try:
            with self._env.begin(write=True) as txn:
                await loop.run_in_executor(None, txn.delete, key.encode())
        except lmdb.Error as e:
            logger.error(f"Error deleting item from LMDB: {e}")
            self._metrics.record_error()

    async def delete_old_entries(self, ratio_to_delete: float = 0.05) -> None:
        """
        Deletes a fraction of the oldest entries from the cache.
        This is a basic strategy and might need refinement for specific use cases.
        A more robust approach would involve tracking access times or using an LRU policy.

        Args:
            ratio_to_delete: The fraction of entries to delete (e.g., 0.05 for 5%).
        """
        logger.info(f"Attempting to delete {ratio_to_delete*100}% of entries from cache.")
        loop = asyncio.get_event_loop()
        try:
            keys_to_delete = []
            with self._env.begin() as txn:
                cursor = txn.cursor()
                # Iterate over all entries to find candidates for deletion
                # This can be inefficient for very large databases.
                # A more sophisticated approach might involve sampling or secondary indexes.
                num_entries = await loop.run_in_executor(None, txn.stat)['entries']
                num_to_delete = int(num_entries * ratio_to_delete)

                if num_to_delete == 0 and num_entries > 0 and ratio_to_delete > 0:
                    num_to_delete = 1 # Ensure at least one entry is deleted if possible

                if num_to_delete == 0:
                    logger.info("No entries to delete or database is empty.")
                    return

                # For simplicity, we'll delete based on iteration order, which often
                # corresponds to insertion order for LMDB if keys are somewhat sequential
                # or if no major rebalancing has occurred.
                # For a truly "oldest" strategy, timestamps would need to be stored with entries.
                # We are now storing expiry times, so we can use that.
                candidates_for_deletion = []
                for key_bytes, value_bytes in cursor:
                    try:
                        _, expire_at = pickle.loads(value_bytes)
                        candidates_for_deletion.append((expire_at if expire_at is not None else float('inf'), key_bytes))
                    except pickle.PickleError:
                        # Corrupted entry, mark for deletion by giving it highest priority (lowest expiry)
                        candidates_for_deletion.append((float('-inf'), key_bytes))
                    except Exception: # Catch other potential unpickling errors
                        candidates_for_deletion.append((float('-inf'), key_bytes))


                # Sort by expiry time (expired or corrupted first, then oldest)
                candidates_for_deletion.sort()

                keys_to_delete = [key_bytes for _, key_bytes in candidates_for_deletion[:num_to_delete]]

            if keys_to_delete:
                with self._env.begin(write=True) as txn:
                    for key_bytes in keys_to_delete:
                        try:
                            txn.delete(key_bytes)
                            logger.debug(f"Deleted old entry with key: {key_bytes.decode(errors='ignore')}")
                        except lmdb.Error as e:
                            logger.error(f"Error deleting entry {key_bytes.decode(errors='ignore')} during cleanup: {e}")
                            self._metrics.record_error()
                logger.info(f"Deleted {len(keys_to_delete)} old entries from the cache.")
            else:
                logger.info("No old entries found to delete.")

        except lmdb.Error as e:
            logger.error(f"Error during deletion of old entries: {e}")
            self._metrics.record_error()
        except Exception as e: # Catch any other unexpected errors
            logger.error(f"Unexpected error during deletion of old entries: {e}")
            self._metrics.record_error()


    async def close(self) -> None:
        """Closes the LMDB environment."""
        if self._env:
            await asyncio.get_event_loop().run_in_executor(None, self._env.close)
            self._env = None

    def get_metrics(self) -> dict:
        """Returns the current cache metrics."""
        return self._metrics.get_metrics()

    async def clear(self) -> None:
        """Clears all entries from the cache."""
        loop = asyncio.get_event_loop()
        try:
            with self._env.begin(write=True) as txn:
                # Iterate over all databases (though we use the main one by default)
                # For the main unnamed database, db_name is None
                db = self._env.open_db(db=None, txn=txn)
                await loop.run_in_executor(None, txn.drop, db, False) # False means delete, not just empty
            logger.info("Cache cleared successfully.")
        except lmdb.Error as e:
            logger.error(f"Error clearing cache: {e}")
            self._metrics.record_error()

    async def __aenter__(self) -> "AsyncLMDBCacheWrapper":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()

    async def check_expired_entries(self) -> None:
        """Checks for and deletes expired entries."""
        logger.debug("Running background task to check for expired entries.")
        loop = asyncio.get_event_loop()
        keys_to_delete = []
        current_time = time.time()
        try:
            with self._env.begin() as txn:
                cursor = txn.cursor()
                for key_bytes, value_bytes in cursor:
                    try:
                        _, expire_at = pickle.loads(value_bytes)
                        if expire_at is not None and expire_at < current_time:
                            keys_to_delete.append(key_bytes)
                    except pickle.PickleError:
                        # Corrupted entry, add to deletion list
                        keys_to_delete.append(key_bytes)
                    except Exception as e: # Catch other potential unpickling errors
                        logger.warning(f"Could not process entry {key_bytes.decode(errors='ignore')} for expiry check: {e}")
                        keys_to_delete.append(key_bytes)


            if keys_to_delete:
                with self._env.begin(write=True) as txn:
                    for key_bytes in keys_to_delete:
                        try:
                            txn.delete(key_bytes)
                            logger.info(f"Deleted expired entry with key: {key_bytes.decode(errors='ignore')}")
                        except lmdb.Error as e:
                            logger.error(f"Error deleting expired entry {key_bytes.decode(errors='ignore')}: {e}")
                            self._metrics.record_error()
                logger.debug(f"Deleted {len(keys_to_delete)} expired entries.")
            else:
                logger.debug("No expired entries found.")
        except lmdb.Error as e:
            logger.error(f"Error during expired entry check: {e}")
            self._metrics.record_error()
        except Exception as e: # Catch any other unexpected errors
            logger.error(f"Unexpected error during expired entry check: {e}")
            self._metrics.record_error()

    async def start_expiry_check_task(self, interval_seconds: int = 60) -> asyncio.Task:
        """
        Starts a background task that periodically checks for and deletes expired entries.

        Args:
            interval_seconds: How often to check for expired entries, in seconds.

        Returns:
            The asyncio.Task object for the background task.
        """
        async def _expiry_check_loop():
            while True:
                await asyncio.sleep(interval_seconds)
                if self._env: # Only run if the environment is active
                    await self.check_expired_entries()
                else:
                    logger.info("LMDB environment closed, stopping expiry check task.")
                    break
        logger.info(f"Starting background expiry check task with interval {interval_seconds}s.")
        return asyncio.create_task(_expiry_check_loop())
