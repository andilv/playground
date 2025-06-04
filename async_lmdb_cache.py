import lmdb
import threading
import time
import asyncio
import msgpack
import logging
import random # Added for sampling
from cachetools import TTLCache
from typing import Optional, Any, Tuple, Callable

import queue
import concurrent.futures

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Command constants for LMDBWorkerThread
CMD_INIT_ENV = "INIT_ENV"
CMD_CLOSE_ENV = "CLOSE_ENV"
CMD_STOP_WORKER = "STOP_WORKER"
CMD_GET_VALUE = "GET_VALUE"
CMD_PUT_VALUE = "PUT_VALUE"
CMD_DELETE_VALUE = "DELETE_VALUE"
CMD_GET_ENV_STATS = "GET_ENV_STATS"
CMD_GET_DB_STATS = "GET_DB_STATS"
CMD_DELETE_EXPIRED_BATCH = "DELETE_EXPIRED_BATCH"

class LMDBWorkerThread(threading.Thread):
    def __init__(self, base_wrapper_metrics_ref):
        super().__init__(daemon=True)
        self.name = "LMDBWorkerThread"
        logger.info(f"LMDBWorkerThread {self.name}: __init__ called.")
        self.request_queue = queue.Queue()
        self.env = None
        self.db = None
        self.lru_db = None
        self._lmdb_lru_db_name_bytes = b'lru_access_times'
        self._lmdb_max_keys = float('inf')
        self._lmdb_lru_sample_size = 5000
        self.metrics_ref = base_wrapper_metrics_ref

    def _encode_value(self, value: Any, expire_at: int) -> bytes:
        return msgpack.packb({'expire_at': expire_at, 'value': value}, use_bin_type=True)

    def _decode_value(self, data: bytes) -> Tuple[Optional[int], Any]:
        print(f"DEBUG MSGFIX DECODE: Received data (hex): {data.hex() if data else 'None'}", flush=True)
        if not data:
            print("DEBUG MSGFIX DECODE: Data is None or empty, returning None, None", flush=True)
            return None, None
        try:
            # Attempt to unpack the entire data
            unpacked = msgpack.unpackb(data, raw=False)
            print(f"DEBUG MSGFIX DECODE: Unpacked successfully: {unpacked}", flush=True)
            expire_at = unpacked.get('expire_at')
            value = unpacked.get('value')
            # print(f"DEBUG MSGFIX DECODE: Returning expire_at={expire_at}, value type={type(value)}", flush=True) # Too verbose
            return expire_at if expire_at != 0 else None, value
        except msgpack.ExtraData as e_extra:
            print(f"DEBUG MSGFIX DECODE: msgpack.ExtraData caught: {e_extra}", flush=True)
            print(f"DEBUG MSGFIX DECODE: Data causing ExtraData (hex): {data.hex()}", flush=True)
            # Try to unpack just the beginning part if it's ExtraData
            try:
                unpacker = msgpack.Unpacker(raw=False)
                unpacker.feed(data)
                first_obj = unpacker.unpack()
                print(f"DEBUG MSGFIX DECODE: First object in ExtraData: {first_obj}", flush=True)
                offset = unpacker.tell()
                print(f"DEBUG MSGFIX DECODE: Offset after first object: {offset}", flush=True)
                if offset < len(data):
                    remaining_data = data[offset:]
                    print(f"DEBUG MSGFIX DECODE: Remaining data (hex): {remaining_data.hex()}", flush=True)
            except Exception as e_unpack_detail:
                print(f"DEBUG MSGFIX DECODE: Could not get details from ExtraData: {e_unpack_detail}", flush=True)
            raise # Re-raise the original ExtraData exception
        except msgpack.UnpackException as e_unpack:
            print(f"DEBUG MSGFIX DECODE: msgpack.UnpackException (other than ExtraData) caught: {e_unpack}", flush=True)
            print(f"DEBUG MSGFIX DECODE: Data causing other UnpackException (hex): {data.hex()}", flush=True)
            raise # Re-raise
        except Exception as e_gen:
            print(f"DEBUG MSGFIX DECODE: Generic Exception caught during decode: {e_gen}", flush=True)
            print(f"DEBUG MSGFIX DECODE: Data causing Generic Exception (hex): {data.hex()}", flush=True)
            raise

    def _execute_init_env(self, path, map_size, max_dbs, lru_db_name_bytes, lmdb_max_keys, lmdb_lru_sample_size, future):
        try:
            if self.env:
                logger.warning("LMDBWorker: Environment already initialized. Closing old one first.")
                self.env.close()
            logger.info(f"LMDBWorker: Initializing environment at {path} with map_size {map_size}")
            self.env = lmdb.open(path, max_dbs=max_dbs, map_size=map_size, lock=True, max_readers=126, sync=True, subdir=True) # Explicitly set subdir=True
            self.db = self.env.open_db()
            self._lmdb_lru_db_name_bytes = lru_db_name_bytes
            self.lru_db = self.env.open_db(self._lmdb_lru_db_name_bytes)
            self._lmdb_max_keys = lmdb_max_keys if lmdb_max_keys is not None else float('inf')
            self._lmdb_lru_sample_size = lmdb_lru_sample_size
            future.set_result(True)
        except Exception as e:
            logger.error(f"LMDBWorker: Failed to initialize environment: {e}", exc_info=True)
            future.set_exception(e)

    def _execute_close_env(self, future):
        try:
            if self.env:
                logger.info("LMDBWorker: Closing environment. Syncing before close.")
                try:
                    self.env.sync(True) # Force a sync before closing
                    logger.info("LMDBWorker: Environment synced successfully.")
                except Exception as sync_e:
                    logger.error(f"LMDBWorker: Exception during env sync before close: {sync_e}", exc_info=True)
                self.env.close()
                self.env = None; self.db = None; self.lru_db = None
            future.set_result(True)
        except Exception as e:
            logger.error(f"LMDBWorker: Exception during close_env: {e}", exc_info=True)
            future.set_exception(e)

    def _execute_get_value(self, db_ref_name, key_b, future):
        try:
            if not self.env: raise lmdb.Error("LMDB environment not initialized.")
            db_to_use = self.db if db_ref_name == 'main' else self.lru_db
            with self.env.begin(db=db_to_use) as txn: value_b = txn.get(key_b)
            future.set_result(value_b)
        except Exception as e: future.set_exception(e)

    def _execute_put_value(self, db_ref_name, key_b, value_b, future, is_update_access_time_op=False):
        try:
            if not self.env: raise lmdb.Error("LMDB environment not initialized.")
            db_to_use = self.db if db_ref_name == 'main' else self.lru_db
            if db_ref_name == 'main' and not is_update_access_time_op and self._lmdb_max_keys != float('inf'):
                with self.env.begin(db=self.db, write=True) as wtxn_main:
                    current_entries = wtxn_main.stat()['entries']
                    if current_entries >= self._lmdb_max_keys:
                        logger.warning(f"LMDBWorker: Max keys {self._lmdb_max_keys} reached (current: {current_entries}). Initiating LRU eviction.")
                        self._perform_lru_eviction_internal(wtxn_main)
                        current_entries_after_evict = wtxn_main.stat()['entries']
                        if current_entries_after_evict >= self._lmdb_max_keys:
                            logger.error(f"LMDBWorker: Max keys limit {self._lmdb_max_keys} still reached after LRU eviction (current {current_entries_after_evict}). Put for {key_b.decode('utf-8','ignore')} may cause overfill or fail.")
                    logger.debug(f"LMDBWorker: Putting key {key_b!r}, value {value_b!r} (original value type: {type(value_b)}).")
                    wtxn_main.put(key_b, value_b)
                self.env.sync(True) # Explicit sync after main DB put
                future.set_result(True); return
            logger.debug(f"LMDBWorker: Putting key {key_b!r}, value {value_b!r} (original value type: {type(value_b)}).")
            with self.env.begin(db=db_to_use, write=True) as txn: txn.put(key_b, value_b)
            self.env.sync(True) # Explicit sync after other DB put
            future.set_result(True)
        except Exception as e: future.set_exception(e)

    def _execute_delete_value(self, db_ref_name, key_b, future):
        try:
            if not self.env: raise lmdb.Error("LMDB environment not initialized.")
            db_to_use = self.db if db_ref_name == 'main' else self.lru_db
            with self.env.begin(db=db_to_use, write=True) as txn: deleted = txn.delete(key_b)
            future.set_result(deleted)
        except Exception as e: future.set_exception(e)

    def _execute_get_env_stats(self, future):
        try:
            if not self.env: raise lmdb.Error("LMDB environment not initialized.")
            future.set_result(self.env.stat())
        except Exception as e: future.set_exception(e)

    def _execute_get_db_stats(self, db_ref_name, future):
        try:
            if not self.env: raise lmdb.Error("LMDB environment not initialized.")
            db_to_use = self.db if db_ref_name == 'main' else self.lru_db
            if not db_to_use: raise ValueError(f"Unknown DB reference for stats: {db_ref_name}")
            with self.env.begin(db=db_to_use) as txn: future.set_result(txn.stat())
        except Exception as e: future.set_exception(e)

    def _perform_lru_eviction_internal(self, main_wtxn: lmdb.Transaction):
        evicted_count = 0
        keys_to_evict_from_sample = []
        current_entries = main_wtxn.stat()['entries'] # main_wtxn is for self.db

        target_eviction_count = current_entries - int(self._lmdb_max_keys * 0.90)
        if target_eviction_count <= 0:
            return

        logger.info(f"LMDBWorker LRU Eviction: Targeting {target_eviction_count} evictions from sample of {self._lmdb_lru_sample_size} (current: {current_entries}, max: {self._lmdb_max_keys}).")
        sample_access_times = []

        try:
            # Step 1: Sample LRU candidates using a separate READ transaction on lru_db
            if not self.lru_db: raise lmdb.Error("LRU DB not available for eviction sampling.")
            with self.env.begin(db=self.lru_db) as lru_txn_read: # Read-only transaction
                cursor = lru_txn_read.cursor()
                for key_b_lru, access_time_b in cursor:
                    if len(sample_access_times) >= self._lmdb_lru_sample_size:
                        break
                    try:
                        access_time = float(access_time_b.decode('utf-8'))
                        sample_access_times.append({'key': key_b_lru, 'time': access_time})
                    except ValueError:
                        logger.warning(f"LMDBWorker: Corrupt access time for key {key_b_lru.decode('utf-8', errors='ignore')} in lru_db. Skipping.")

            if not sample_access_times:
                logger.info("LMDBWorker LRU Eviction: No samples found in lru_db to evict from.")
                return

            sample_access_times.sort(key=lambda x: x['time'])
            keys_to_evict_from_sample = [item['key'] for item in sample_access_times[:target_eviction_count]]

            # Step 2: Perform deletions using the existing main_wtxn (WRITE transaction)
            if keys_to_evict_from_sample:
                # Use the main_wtxn to operate on both self.db and self.lru_db via cursors
                with main_wtxn.cursor(db=self.lru_db) as lru_cursor_write: # Cursor for lru_db on main_wtxn
                    for key_b_evict in keys_to_evict_from_sample:
                        # Delete from main DB (self.db) using main_wtxn directly
                        # Ensure to specify the db handle for main_wtxn.delete if it's not the default for the txn
                        if main_wtxn.delete(key_b_evict, db=self.db):
                            # If successful, delete from LRU DB using cursor on main_wtxn
                            # The key must exist for lru_cursor_write.delete() to succeed without error
                            if lru_cursor_write.set_key(key_b_evict): # Check if key exists before deleting
                                lru_cursor_write.delete()
                            evicted_count += 1
                        else:
                            # Key not in main_db (maybe already deleted), but try cleaning from lru_db
                            if lru_cursor_write.set_key(key_b_evict): # Check if key exists
                               lru_cursor_write.delete()

                if evicted_count > 0 and self.metrics_ref:
                    self.metrics_ref.lmdb_lru_evictions += evicted_count
                    logger.info(f"LMDBWorker LRU Eviction: Successfully deleted {evicted_count} entries based on sample.")
            else:
                logger.info("LMDBWorker LRU Eviction: No entries selected for eviction from the sample.")
        except Exception as e:
            logger.error(f"LMDBWorker: Error during LRU eviction process: {e}", exc_info=True)
            # Do not re-raise, as this is called from within another transaction context (_execute_put_value)

    def _perform_delete_expired_internal(self, future):
        now = time.time(); deleted_count = 0
        keys_for_main_db_delete, keys_for_lru_db_delete = [], []
        batch_size = 1000
        try:
            if not self.env or not self.db or not self.lru_db: raise lmdb.Error("LMDB env/dbs not initialized for expired deletion.")
            with self.env.begin(db=self.db) as main_txn_read:
                cursor = main_txn_read.cursor()
                for key_b, data in cursor:
                    try:
                        expire_at, _ = self._decode_value(data)
                        if expire_at is not None and expire_at < now:
                            keys_for_main_db_delete.append(key_b); keys_for_lru_db_delete.append(key_b)
                            if len(keys_for_main_db_delete) >= batch_size: break
                    except msgpack.UnpackException as e_unpack_outer:
                        print(f"DEBUG MSGFIX KEYLOG: Caught in _perform_delete_expired_internal: {e_unpack_outer}", flush=True)
                        print(f"DEBUG MSGFIX KEYLOG: Problematic key: {key_b.decode('utf-8', errors='ignore')}", flush=True)
                        keys_for_main_db_delete.append(key_b)
                        keys_for_lru_db_delete.append(key_b)
                        if len(keys_for_main_db_delete) >= batch_size:
                            print(f"DEBUG MSGFIX KEYLOG: Batch limit reached in except block of _perform_delete_expired_internal.", flush=True)
                            break
            if keys_for_main_db_delete:
                with self.env.begin(db=self.db, write=True) as main_txn_write:
                    for key_b_del in keys_for_main_db_delete:
                        if main_txn_write.delete(key_b_del): deleted_count += 1
                with self.env.begin(db=self.lru_db, write=True) as lru_txn_write:
                    for key_b_del_lru in keys_for_lru_db_delete: lru_txn_write.delete(key_b_del_lru)
            if deleted_count > 0 and self.metrics_ref:
                self.metrics_ref.lmdb_deletions += deleted_count
                logger.info(f"LMDBWorker: Deleted {deleted_count} expired entries.")
            future.set_result(deleted_count)
        except Exception as e:
            logger.error(f"LMDBWorker: Error during expired entry deletion: {e}", exc_info=True)
            future.set_exception(e)

    def run(self):
        logger.info(f"LMDBWorkerThread {self.name}: run() method started.")
        try:
            while True:
                future_obj, request_tuple = None, None
                try:
                    logger.debug(f"LMDBWorkerThread {self.name}: Waiting for request...")
                    request_tuple = self.request_queue.get()
                    logger.debug(f"LMDBWorkerThread {self.name}: Got request: {request_tuple[0] if request_tuple else 'EMPTY?!'}")
                    command, args, future_obj = request_tuple[0], request_tuple[1:-1], request_tuple[-1]

                    if command == CMD_STOP_WORKER:
                        logger.info(f"LMDBWorkerThread {self.name}: STOP_WORKER command received. Terminating.")
                        # if self.env: # Remove this block
                        #     internal_close_future = concurrent.futures.Future()
                        #     self._execute_close_env(internal_close_future)
                        #     try: internal_close_future.result(timeout=5)
                        #     except Exception as e_close: logger.error(f"LMDBWorker: Exception closing env during STOP: {e_close}")
                        if future_obj: future_obj.set_result(True); break
                    if not self.env and command != CMD_INIT_ENV:
                        err_msg = f"LMDBWorker: Cmd '{command}' received but env not initialized."
                        logger.error(err_msg)
                        if future_obj: future_obj.set_exception(lmdb.Error(err_msg))
                        self.request_queue.task_done(); continue
                    if command == CMD_INIT_ENV: self._execute_init_env(*args, future_obj)
                    elif command == CMD_CLOSE_ENV: self._execute_close_env(future_obj)
                    elif command == CMD_GET_VALUE: self._execute_get_value(*args, future_obj)
                    elif command == CMD_PUT_VALUE:
                        db_ref, key_b, value_b = args; is_update_op = (db_ref == 'lru')
                        self._execute_put_value(db_ref, key_b, value_b, future_obj, is_update_access_time_op=is_update_op)
                    elif command == CMD_DELETE_VALUE: self._execute_delete_value(*args, future_obj)
                    elif command == CMD_GET_ENV_STATS: self._execute_get_env_stats(future_obj)
                    elif command == CMD_GET_DB_STATS: self._execute_get_db_stats(*args, future_obj)
                    elif command == CMD_DELETE_EXPIRED_BATCH: self._perform_delete_expired_internal(future_obj)
                    else:
                        err_msg = f"LMDBWorker: Unknown command '{command}'"; logger.error(err_msg)
                        if future_obj: future_obj.set_exception(ValueError(err_msg))
                    self.request_queue.task_done()
                except Exception as e:
                    logger.error(f"LMDBWorkerThread: Error processing request {request_tuple if request_tuple else 'unknown'}: {e}", exc_info=True)
                    if future_obj and not future_obj.done(): future_obj.set_exception(e)
                    if request_tuple: self.request_queue.task_done()
        except Exception as e: logger.critical(f"LMDBWorkerThread: Unhandled exception in main run loop, thread terminating: {e}", exc_info=True)
        finally:
            if self.env:
                logger.warning("LMDBWorkerThread: Exiting run loop, env might still be open. Final close attempt.")
                final_close_future = concurrent.futures.Future()
                self._execute_close_env(final_close_future)
                try: final_close_future.result(timeout=5)
                except Exception: logger.error("LMDBWorkerThread: Exception during final env close on exit.", exc_info=True)
            logger.info("LMDBWorkerThread finished.")

class CustomTTLCache(TTLCache):
    def __init__(self, maxsize, ttl, timer=time.monotonic, getsizeof=lambda _: 1):
        super().__init__(maxsize, ttl, timer, getsizeof)
        self.eviction_callback = None
    def popitem(self):
        key, value = super().popitem()
        if self.eviction_callback:
            try: self.eviction_callback(key, value)
            except Exception as e: logger.error(f"Error in TTLCache eviction_callback for key {key}: {e}", exc_info=True)
        return key, value

class Metrics:
    def __init__(self):
        self.lru_hits, self.lru_misses, self.lmdb_hits, self.lmdb_misses = 0,0,0,0
        self.lru_evictions, self.lmdb_deletions, self.lmdb_lru_evictions = 0,0,0
        self.get_latency, self.put_latency = [], []
    def report(self):
        return {"lru_hits":self.lru_hits,"lru_misses":self.lru_misses,"lmdb_hits":self.lmdb_hits,
                "lmdb_misses":self.lmdb_misses,"lru_evictions":self.lru_evictions,
                "lmdb_deletions":self.lmdb_deletions,"lmdb_lru_evictions":self.lmdb_lru_evictions,
                "avg_get_latency_ms": (sum(self.get_latency)/len(self.get_latency)*1000) if self.get_latency else 0,
                "avg_put_latency_ms": (sum(self.put_latency)/len(self.put_latency)*1000) if self.put_latency else 0}

class BaseLMDBCacheWrapper:
    def __init__(self, path: str, lru_capacity: int = 1000, lmdb_max_keys: Optional[int] = 1000000,
                 map_size: int = 10**10, default_ttl: int = 3600, cleanup_interval: int = 600,
                 lmdb_lru_db_name: bytes = b'lru_access_times', lmdb_lru_sample_size: int = 5000):
        self.metrics = Metrics()
        self.lmdb_operation_timeout = 10
        self._base_is_closing = False
        self.lock = threading.Lock()
        self._lmdb_max_keys_config = lmdb_max_keys if lmdb_max_keys is not None else float('inf')
        self._lmdb_lru_sample_size_config = lmdb_lru_sample_size
        # Ensure the directory is clean before starting the worker and initializing LMDB
        import shutil
        import os
        if os.path.exists(path):
            logger.info(f"BaseLMDBCacheWrapper: Cleaning up existing LMDB directory at {path} before worker init.")
            shutil.rmtree(path)
        os.makedirs(path, exist_ok=True) # Ensure directory exists

        self.lmdb_worker = LMDBWorkerThread(base_wrapper_metrics_ref=self.metrics)
        self.lmdb_worker.start()
        init_future = concurrent.futures.Future()
        lru_db_name_b = lmdb_lru_db_name if isinstance(lmdb_lru_db_name, bytes) else lmdb_lru_db_name.encode('utf-8')
        self.lmdb_worker.request_queue.put((
            CMD_INIT_ENV, path, map_size, 2, lru_db_name_b,
            self._lmdb_max_keys_config, self._lmdb_lru_sample_size_config, init_future
        ))
        try:
            if not init_future.result(timeout=self.lmdb_operation_timeout):
                raise ConnectionError("LMDBWorker failed to initialize (result False/None)")
            logger.info("BaseLMDBCacheWrapper: LMDB environment initialized successfully via worker.")
        except Exception as e:
            logger.critical(f"BaseLMDBCacheWrapper: Failed to initialize LMDB via worker: {e}", exc_info=True)
            self._stop_lmdb_worker_sync_on_error()
            raise ConnectionError(f"Failed to initialize LMDB via worker: {e}") from e
        self.lru_cache = CustomTTLCache(maxsize=lru_capacity, ttl=default_ttl)
        self.lru_cache.eviction_callback = self._on_lru_evict_callback # Set callback after init
        self.default_ttl = default_ttl
        self.cleanup_interval = cleanup_interval

    def _on_lru_evict_callback(self, key, value): self.metrics.lru_evictions += 1

    def _stop_lmdb_worker_sync_on_error(self):
        if self.lmdb_worker and self.lmdb_worker.is_alive():
            logger.info("Attempting to stop LMDBWorker due to error...")
            stop_future = concurrent.futures.Future()
            self.lmdb_worker.request_queue.put((CMD_STOP_WORKER, stop_future))
            try: stop_future.result(timeout=2)
            except Exception: logger.error("Exception/Timeout stopping worker on error.", exc_info=True)
            self.lmdb_worker.join(timeout=2)
            if self.lmdb_worker.is_alive(): logger.error("LMDBWorker did not terminate on error.")

    def _submit_lmdb_command(self, command_tuple_with_args, expect_result=True, timeout_override=None):
        # command_tuple_with_args is (COMMAND, arg1, arg2, ...)
        with self.lock:
            if self._base_is_closing:
                logger.warning(f"LMDB command {command_tuple_with_args[0]} not submitted: cache is closing.")
                raise lmdb.Error("Cache is closing.")
        future = concurrent.futures.Future()
        full_request = (*command_tuple_with_args, future) # Add future to the end
        self.lmdb_worker.request_queue.put(full_request)
        op_timeout = timeout_override if timeout_override is not None else self.lmdb_operation_timeout
        try:
            result = future.result(timeout=op_timeout)
            return result if expect_result else True
        except TimeoutError:
            logger.error(f"LMDB command {command_tuple_with_args[0]} timed out after {op_timeout}s.")
            raise lmdb.Error(f"LMDB operation {command_tuple_with_args[0]} timed out.") from TimeoutError
        except Exception as e: raise

    def _encode_key(self, key: str) -> bytes: return key.encode('utf-8')
    def _encode_value(self, value: Any, expire_at: int) -> bytes:
        return msgpack.packb({'expire_at': expire_at, 'value': value}, use_bin_type=True)
    def _decode_value(self, data: bytes) -> Tuple[Optional[int], Any]:
        if not data: return None, None
        unpacked = msgpack.unpackb(data, raw=False); expire_at = unpacked.get('expire_at')
        return expire_at if expire_at != 0 else None, unpacked.get('value')

    def _lmdb_get_operation(self, db_ref_name: str, key_b: bytes) -> Optional[bytes]:
        return self._submit_lmdb_command((CMD_GET_VALUE, db_ref_name, key_b))

    def _lmdb_put_operation(self, db_ref_name: str, key_b: bytes, value_b: bytes):
        self._submit_lmdb_command((CMD_PUT_VALUE, db_ref_name, key_b, value_b), expect_result=False)

    def _lmdb_delete_operation(self, db_ref_name: str, key_b: bytes) -> bool:
        return self._submit_lmdb_command((CMD_DELETE_VALUE, db_ref_name, key_b))

    def _lmdb_delete_expired_batch_operation(self) -> int:
        return self._submit_lmdb_command((CMD_DELETE_EXPIRED_BATCH,))

    def get_environment_stats(self) -> dict:
        """Retrieves statistics for the LMDB environment via the worker thread."""
        if not self.lmdb_worker or not self.lmdb_worker.is_alive():
            raise lmdb.Error("LMDB worker is not running.")
        return self._submit_lmdb_command((CMD_GET_ENV_STATS,))

    def get_database_stats(self, db_ref_name: str) -> dict:
        """Retrieves statistics for a specific database ('main' or 'lru') via the worker thread."""
        if not self.lmdb_worker or not self.lmdb_worker.is_alive():
            raise lmdb.Error("LMDB worker is not running.")
        if db_ref_name not in ['main', 'lru']:
            raise ValueError("db_ref_name must be 'main' or 'lru'")
        return self._submit_lmdb_command((CMD_GET_DB_STATS, db_ref_name))

    def close(self):
        logger.info(f"BaseLMDBCacheWrapper ({type(self).__name__}): Initiating close sequence.")
        with self.lock:
            if self._base_is_closing: logger.info(f"BaseLMDBCacheWrapper ({type(self).__name__}): Already closing."); return
            self._base_is_closing = True
        if self.lmdb_worker and self.lmdb_worker.is_alive():
            logger.info(f"BaseLMDBCacheWrapper ({type(self).__name__}): Sending CLOSE_ENV and STOP_WORKER to worker.")
            close_env_future = concurrent.futures.Future()
            self.lmdb_worker.request_queue.put((CMD_CLOSE_ENV, close_env_future))
            try: close_env_future.result(timeout=self.lmdb_operation_timeout)
            except Exception as e: logger.error(f"Base ({type(self).__name__}): Error waiting for worker CLOSE_ENV: {e}", exc_info=True)
            stop_worker_future = concurrent.futures.Future()
            self.lmdb_worker.request_queue.put((CMD_STOP_WORKER, stop_worker_future))
            try: stop_worker_future.result(timeout=self.lmdb_operation_timeout)
            except Exception as e: logger.error(f"Base ({type(self).__name__}): Error waiting for worker STOP_WORKER: {e}", exc_info=True)
            self.lmdb_worker.join(timeout=self.lmdb_operation_timeout + 2)
            if self.lmdb_worker.is_alive(): logger.error(f"Base ({type(self).__name__}): LMDBWorkerThread did not terminate.")
        else: logger.info(f"Base ({type(self).__name__}): LMDBWorkerThread not running or already joined.")
        logger.info(f"BaseLMDBCacheWrapper ({type(self).__name__}): Close sequence complete.")

class AsyncLMDBCacheWrapper(BaseLMDBCacheWrapper):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._cleanup_task: Optional[asyncio.Task] = None
        self._stop_cleanup = asyncio.Event()

    async def _dispatch_lmdb_op_async(self, command_tuple_with_args, fire_and_forget=False, timeout_override=None):
        # command_tuple_with_args is (COMMAND, arg1, arg2, ...)
        if self._base_is_closing:
            logger.warning(f"AsyncLMDB: Command {command_tuple_with_args[0]} not submitted: cache closing.")
            # Provide default "error" values based on command type
            if command_tuple_with_args[0] == CMD_GET_VALUE: return None
            if command_tuple_with_args[0] == CMD_DELETE_EXPIRED_BATCH: return 0
            if command_tuple_with_args[0] == CMD_DELETE_VALUE: return False # delete returns bool
            # For PUT or other ops that don't return specific values on success,
            # raising an error is appropriate as they can't proceed.
            raise lmdb.Error("Cache is closing, operation aborted.")

        loop = asyncio.get_running_loop()
        future = concurrent.futures.Future()
        full_request = (*command_tuple_with_args, future)
        self.lmdb_worker.request_queue.put(full_request)

        if fire_and_forget:
            # For fire-and-forget, we might want to handle potential exceptions from the future
            # to avoid unhandled future exceptions, but not await the result.
            def _log_future_exception(f):
                try:
                    f.result(timeout=0) # Check for immediate exception without blocking
                except Exception as e:
                    logger.error(f"AsyncLMDB: Fire-and-forget command {command_tuple_with_args[0]} resulted in exception: {e}", exc_info=True)
            future.add_done_callback(_log_future_exception)
            return None

        op_timeout = timeout_override if timeout_override is not None else self.lmdb_operation_timeout
        try:
            return await loop.run_in_executor(None, future.result, op_timeout)
        except TimeoutError:
            logger.error(f"AsyncLMDB: Command {command_tuple_with_args[0]} timed out after {op_timeout}s.")
            raise lmdb.Error(f"LMDB operation {command_tuple_with_args[0]} timed out.") from TimeoutError
        except Exception as e: raise

    async def get(self, key: str) -> Optional[Any]:
        start_time = time.monotonic()
        if self._base_is_closing:
            logger.warning(f"Cache is closing. Get for key '{key}' (from TTLCache or None).")
            if key in self.lru_cache:
                self.metrics.lru_hits += 1; self.metrics.get_latency.append(time.monotonic() - start_time)
                return self.lru_cache[key]
            self.metrics.get_latency.append(time.monotonic() - start_time); return None
        if key in self.lru_cache:
            val = self.lru_cache[key]; self.metrics.lru_hits += 1
            if not self._base_is_closing:
                 key_b_lru = self._encode_key(key)
                 # Restoring actual call
                 asyncio.create_task(self._dispatch_lmdb_op_async((CMD_PUT_VALUE, 'lru', key_b_lru, str(time.time()).encode('utf-8')), fire_and_forget=True))
            self.metrics.get_latency.append(time.monotonic() - start_time); return val
        self.metrics.lru_misses += 1; key_b = self._encode_key(key)
        try: data_bytes = await self._dispatch_lmdb_op_async((CMD_GET_VALUE, 'main', key_b))
        except lmdb.Error as e: # Includes timeout or worker error
            logger.error(f"Async get: LMDB error for key '{key}': {e}", exc_info=True)
            self.metrics.lmdb_misses += 1; self.metrics.get_latency.append(time.monotonic() - start_time); return None
        if data_bytes is None:
            self.metrics.lmdb_misses += 1; self.metrics.get_latency.append(time.monotonic() - start_time); return None
        try:
            expire_at, value = self._decode_value(data_bytes)
        except msgpack.UnpackException as e:
            print(f"DEBUG MSGFIX KEYLOG ASYNC GET: Caught in AsyncLMDBCacheWrapper.get: {e}", flush=True)
            print(f"DEBUG MSGFIX KEYLOG ASYNC GET: Problematic key: {key_b.decode('utf-8', errors='ignore')}", flush=True) # key_b should be in scope
            return None
        current_time = time.time()
        if expire_at is not None and current_time > expire_at:
            logger.info(f"Key '{key}' found in LMDB but expired (async). Deleting.")
            if not self._base_is_closing:
                asyncio.create_task(self._dispatch_lmdb_op_async((CMD_DELETE_VALUE, 'main', key_b), fire_and_forget=True))
                asyncio.create_task(self._dispatch_lmdb_op_async((CMD_DELETE_VALUE, 'lru', key_b), fire_and_forget=True))
            self.metrics.lmdb_deletions += 1; self.metrics.get_latency.append(time.monotonic() - start_time); return None
        self.metrics.lmdb_hits += 1
        if not self._base_is_closing:
             # Restoring actual call
             asyncio.create_task(self._dispatch_lmdb_op_async((CMD_PUT_VALUE, 'lru', key_b, str(time.time()).encode('utf-8')), fire_and_forget=True))
        try: self.lru_cache[key] = value
        except ValueError: logger.warning(f"Could not add key '{key}' to TTLCache (full/value issue).", exc_info=True)
        self.metrics.get_latency.append(time.monotonic() - start_time); return value

    async def put(self, key: str, value: Any, ttl: Optional[int] = None):
        start_time = time.monotonic()
        if self._base_is_closing:
            logger.warning(f"Cache is closing. Put for key '{key}' rejected."); self.metrics.put_latency.append(time.monotonic() - start_time); return
        actual_ttl = ttl if ttl is not None else self.default_ttl
        expire_at_timestamp = int(time.time() + actual_ttl) if actual_ttl > 0 else 0
        key_b, val_b = self._encode_key(key), self._encode_value(value, expire_at_timestamp)
        try:
            await self._dispatch_lmdb_op_async((CMD_PUT_VALUE, 'main', key_b, val_b))
            if not self._base_is_closing:
                # Restoring actual call
                asyncio.create_task(self._dispatch_lmdb_op_async((CMD_PUT_VALUE, 'lru', key_b, str(time.time()).encode('utf-8')), fire_and_forget=True))
            self.lru_cache[key] = value
        except lmdb.Error as e: logger.error(f"LMDB error on async put for key '{key}': {e}", exc_info=True); raise
        except Exception as e: logger.error(f"Error during async put for key '{key}': {e}", exc_info=True); raise
        self.metrics.put_latency.append(time.monotonic() - start_time)

    async def delete(self, key: str) -> None:
        if self._base_is_closing: logger.warning(f"Cache is closing. Delete for key '{key}' rejected."); return
        key_b = self._encode_key(key)
        try:
            await self._dispatch_lmdb_op_async((CMD_DELETE_VALUE, 'main', key_b))
            await self._dispatch_lmdb_op_async((CMD_DELETE_VALUE, 'lru', key_b))
        except lmdb.Error as e: logger.error(f"LMDB error on async delete for key '{key}': {e}", exc_info=True); raise
        except Exception as e: logger.error(f"Error during async delete for key '{key}': {e}", exc_info=True); raise
        self.lru_cache.pop(key, None); self.metrics.lmdb_deletions += 1; logger.info(f"Async deleted key '{key}' from cache.")

    async def delete_old_entries(self, batch_size: int = 1000) -> int:
        if self._base_is_closing: logger.info("Cache is closing, delete_old_entries skipped."); return 0
        try: return await self._dispatch_lmdb_op_async((CMD_DELETE_EXPIRED_BATCH,))
        except lmdb.Error as e: logger.error(f"LMDB error during delete_old_entries: {e}", exc_info=True); return 0
        except Exception as e: logger.error(f"Error during delete_old_entries: {e}", exc_info=True); return 0

    async def get_environment_stats_async(self) -> dict:
        return await self._dispatch_lmdb_op_async((CMD_GET_ENV_STATS,))

    async def get_database_stats_async(self, db_ref_name: str) -> dict:
        if db_ref_name not in ['main', 'lru']: raise ValueError("db_ref_name must be 'main' or 'lru'")
        return await self._dispatch_lmdb_op_async((CMD_GET_DB_STATS, db_ref_name))

    async def start_background_cleanup(self):
        if self._base_is_closing: logger.warning("Cannot start background cleanup: Cache is closing."); return
        if not hasattr(self, 'lmdb_worker') or not self.lmdb_worker.is_alive():
             logger.warning("Cannot start background cleanup: LMDB worker not ready.")
             return
        if self._cleanup_task is None or self._cleanup_task.done():
            self._stop_cleanup.clear(); self._cleanup_task = asyncio.create_task(self._cleanup_loop())
            logger.info("Async cache background cleanup task initiated.")
        else: logger.info("Async cache background cleanup task is already running.")

    async def _cleanup_loop(self):
        logger.info(f"Async cleanup loop started. Interval: {self.cleanup_interval}s.")
        while not self._stop_cleanup.is_set():
            if self._base_is_closing: logger.info("Async cleanup loop: Cache is closing. Exiting."); break
            try:
                deleted_count = await self.delete_old_entries()
                if deleted_count > 0: logger.info(f"[Async Cleanup] Cycle complete. Deleted {deleted_count} expired entries.")
                else: logger.debug("[Async Cleanup] Cycle complete. No expired entries found.")
            except Exception as e: logger.error(f"[Async Cleanup] Error during cleanup cycle: {e}", exc_info=True)
            try:
                await asyncio.wait_for(self._stop_cleanup.wait(), timeout=self.cleanup_interval)
                if self._stop_cleanup.is_set(): logger.info("Async cleanup loop: Stop event received, exiting."); break
            except asyncio.TimeoutError: logger.debug("Async cleanup loop: Interval ended, proceeding to next cycle.")
            except Exception as e: logger.error(f"[Async Cleanup] Error in wait logic: {e}", exc_info=True); break
        logger.info("Async cleanup loop finished.")

    async def stop_background_cleanup(self):
        if self._cleanup_task and not self._cleanup_task.done():
            logger.info("Stopping async cache background cleanup task.")
            self._stop_cleanup.set()
            try:
                # Give it a bit more time than cleanup_interval to gracefully shut down
                await asyncio.wait_for(self._cleanup_task, timeout=self.cleanup_interval + 5)
            except asyncio.TimeoutError:
                logger.warning("Async cleanup task did not stop gracefully within timeout.")
            except Exception as e:
                logger.error(f"Error waiting for async cleanup task to stop: {e}", exc_info=True)
            self._cleanup_task = None
        else:
            logger.info("Async cache background cleanup task not running or already stopped.")

    async def close(self):
        logger.info("Closing AsyncLMDBCacheWrapper...")
        # Ensure the background cleanup task is stopped before closing the LMDB environment
        await self.stop_background_cleanup()
        # Base class close() now handles setting _base_is_closing and commanding worker
        await asyncio.to_thread(super().close)
        logger.info("AsyncLMDBCacheWrapper fully closed.")

class SyncLMDBCacheWrapper(BaseLMDBCacheWrapper):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._cleanup_thread: Optional[threading.Thread] = None
        self._stop_cleanup_event = threading.Event()

    def _send_worker_command_sync(self, command_tuple_with_args, timeout_override=None):
        # command_tuple_with_args is (COMMAND, arg1, arg2, ...)
        if self._base_is_closing:
            logger.warning(f"SyncLMDB: Command {command_tuple_with_args[0]} not submitted: cache closing.")
            if command_tuple_with_args[0] == CMD_GET_VALUE: return None
            if command_tuple_with_args[0] == CMD_DELETE_EXPIRED_BATCH: return 0
            raise lmdb.Error("Cache is closing.")
        future = concurrent.futures.Future()
        full_request = (*command_tuple_with_args, future)
        self.lmdb_worker.request_queue.put(full_request)
        op_timeout = timeout_override if timeout_override is not None else self.lmdb_operation_timeout
        try: return future.result(timeout=op_timeout)
        except TimeoutError:
            logger.error(f"SyncLMDB: Command {command_tuple_with_args[0]} timed out after {op_timeout}s.")
            raise lmdb.Error(f"LMDB operation {command_tuple_with_args[0]} timed out.") from TimeoutError
        except Exception as e: raise

    def close(self):
        logger.info("Closing SyncLMDBCacheWrapper...")
        self.stop_background_cleanup()
        super().close()
        logger.info("SyncLMDBCacheWrapper closed.")

    def get(self, key: str) -> Optional[Any]:
        start_time = time.monotonic()
        if self._base_is_closing:
            logger.warning(f"SyncCache is closing. Get for key '{key}' returning None from TTLCache or None.")
            if key in self.lru_cache: return self.lru_cache[key]
            return None
        if key in self.lru_cache:
            val = self.lru_cache[key]; self.metrics.lru_hits += 1
            if not self._base_is_closing:
                self._send_worker_command_sync((CMD_PUT_VALUE, 'lru', self._encode_key(key), str(time.time()).encode('utf-8')))
            self.metrics.get_latency.append(time.monotonic() - start_time); return val
        self.metrics.lru_misses += 1; key_b = self._encode_key(key)
        try: data = self._send_worker_command_sync((CMD_GET_VALUE, 'main', key_b))
        except lmdb.Error as e:
            logger.error(f"Sync get: LMDB error for key '{key}': {e}", exc_info=True)
            self.metrics.lmdb_misses += 1; self.metrics.get_latency.append(time.monotonic() - start_time); return None
        if data is None:
            self.metrics.lmdb_misses += 1; self.metrics.get_latency.append(time.monotonic() - start_time); return None
        try:
            expire_at, value = self._decode_value(data)
        except msgpack.UnpackException as e:
            print(f"DEBUG MSGFIX KEYLOG SYNC GET: Caught in SyncLMDBCacheWrapper.get: {e}", flush=True)
            print(f"DEBUG MSGFIX KEYLOG SYNC GET: Problematic key: {key_b.decode('utf-8', errors='ignore')}", flush=True) # key_b should be in scope
            return None
        current_time = time.time()
        if expire_at is not None and current_time > expire_at:
            logger.info(f"Key '{key}' found in LMDB but expired (sync). Deleting.")
            if not self._base_is_closing:
                self._send_worker_command_sync((CMD_DELETE_VALUE, 'main', key_b))
                self._send_worker_command_sync((CMD_DELETE_VALUE, 'lru', key_b))
            self.lru_cache.pop(key, None); self.metrics.lmdb_deletions += 1
            self.metrics.get_latency.append(time.monotonic() - start_time); return None
        self.metrics.lmdb_hits += 1
        if not self._base_is_closing:
            self._send_worker_command_sync((CMD_PUT_VALUE, 'lru', key_b, str(time.time()).encode('utf-8')))
        try: self.lru_cache[key] = value
        except ValueError: logger.warning(f"Could not add key '{key}' to TTLCache (sync).", exc_info=True)
        self.metrics.get_latency.append(time.monotonic() - start_time); return value

    def put(self, key: str, value: Any, ttl: Optional[int] = None):
        start_time = time.monotonic()
        if self._base_is_closing: logger.warning(f"SyncCache is closing. Put for key '{key}' rejected."); return
        actual_ttl = ttl if ttl is not None else self.default_ttl
        expire_at_timestamp = int(time.time() + actual_ttl) if actual_ttl > 0 else 0
        key_b, val_b = self._encode_key(key), self._encode_value(value, expire_at_timestamp)
        try:
            self._send_worker_command_sync((CMD_PUT_VALUE, 'main', key_b, val_b))
            if not self._base_is_closing:
                self._send_worker_command_sync((CMD_PUT_VALUE, 'lru', key_b, str(time.time()).encode('utf-8')))
            self.lru_cache[key] = value
        except lmdb.Error as e: logger.error(f"LMDB error on sync put for key '{key}': {e}", exc_info=True); raise
        except Exception as e: logger.error(f"Error during sync put for key '{key}': {e}", exc_info=True); raise
        self.metrics.put_latency.append(time.monotonic() - start_time)

    def delete(self, key: str) -> None:
        if self._base_is_closing: logger.warning(f"SyncCache is closing. Delete for key '{key}' rejected."); return
        key_b = self._encode_key(key)
        try:
            self._send_worker_command_sync((CMD_DELETE_VALUE, 'main', key_b))
            self._send_worker_command_sync((CMD_DELETE_VALUE, 'lru', key_b))
        except lmdb.Error as e: logger.error(f"LMDB error on sync delete for key '{key}': {e}", exc_info=True); raise
        except Exception as e: logger.error(f"Error during sync delete for key '{key}': {e}", exc_info=True); raise
        self.lru_cache.pop(key, None); self.metrics.lmdb_deletions += 1; logger.info(f"Sync deleted key '{key}' from cache.")

    def start_background_cleanup(self):
        with self.lock:
            if self._base_is_closing or not hasattr(self, 'lmdb_worker') or not self.lmdb_worker.is_alive():
                logger.warning("Sync background cleanup cannot start: Cache closing or LMDB worker not ready.")
                return
        if self._cleanup_thread is None or not self._cleanup_thread.is_alive():
            self._stop_cleanup_event.clear()
            self._cleanup_thread = threading.Thread(target=self._cleanup_loop_sync, daemon=True)
            self._cleanup_thread.name = "SyncLMDBCacheCleanupThread"; self._cleanup_thread.start()
            logger.info("Sync cache background cleanup thread started.")
        else: logger.info("Sync cache background cleanup thread is already running.")

    def _cleanup_loop_sync(self):
        logger.info(f"Sync cleanup loop started. Interval: {self.cleanup_interval}s.")
        while not self._stop_cleanup_event.is_set():
            with self.lock: # Check _base_is_closing under lock
                if self._base_is_closing or not hasattr(self, 'lmdb_worker') or not self.lmdb_worker.is_alive():
                    logger.warning("Sync cleanup loop: Cache closing or LMDB worker not ready. Stopping loop.")
                    break
            try:
                deleted_count = self._send_worker_command_sync((CMD_DELETE_EXPIRED_BATCH,))
                if deleted_count > 0: logger.info(f"[Sync Cleanup] Cycle complete. Deleted {deleted_count} expired entries.")
                else: logger.debug("[Sync Cleanup] Cycle complete. No expired entries found.")
            except Exception as e: logger.error(f"[Sync Cleanup] Error during cleanup cycle: {e}", exc_info=True)
            if self._stop_cleanup_event.wait(timeout=self.cleanup_interval):
                logger.info("Sync cleanup loop: Stop event received, exiting."); break
            logger.debug("Sync cleanup loop: Interval ended, proceeding to next cycle.")
        logger.info("Sync cleanup loop finished.")
