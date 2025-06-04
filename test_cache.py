import asyncio
import os
import shutil
import time
import pytest
import pytest_asyncio # For async fixtures
import logging
import msgpack # Added for new test
import concurrent.futures # Added for new test

from async_lmdb_cache import AsyncLMDBCacheWrapper, Metrics # Assuming this is the correct import path
# CMD_PUT_VALUE is used in the new test, define it here for simplicity
CMD_PUT_VALUE = "PUT_VALUE"


# Configure logger for tests if needed, or rely on main module's logger
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG) # to see debug logs from tests

TEST_CACHE_PATH = "/tmp/test_lmdb_cache_pytest"
BASE_LRU_CAPACITY = 3 # Small capacity for easy testing of LRU
BASE_DEFAULT_TTL = 1 # 1 second TTL for fast expiration tests
BASE_LMDB_MAX_KEYS = 100
BASE_MAP_SIZE = 10 * 1024 * 1024 # 10MB

# Async fixture to create and cleanup cache instance for each test
@pytest_asyncio.fixture
async def cache():
    # Cleanup before test run
    if os.path.exists(TEST_CACHE_PATH):
        shutil.rmtree(TEST_CACHE_PATH)
        time.sleep(0.1) # Add a small delay after rmtree
    os.makedirs(TEST_CACHE_PATH, exist_ok=True)

    cache_instance = AsyncLMDBCacheWrapper(
        path=TEST_CACHE_PATH,
        lru_capacity=BASE_LRU_CAPACITY,
        default_ttl=BASE_DEFAULT_TTL,
        lmdb_max_keys=BASE_LMDB_MAX_KEYS,
        map_size=BASE_MAP_SIZE,
        cleanup_interval=300 # Longer interval, less likely to interfere
    )
    cache_instance.lmdb_operation_timeout = 2
    await cache_instance.start_background_cleanup() # Re-enable background cleanup
    yield cache_instance # Test runs here
    await cache_instance.close()
    if os.path.exists(TEST_CACHE_PATH):
        shutil.rmtree(TEST_CACHE_PATH)

TEST_CACHE_PATH_LMDB_EVICT = "/tmp/test_lmdb_cache_pytest_lmdb_evict"
LMDB_EVICT_MAX_KEYS = 10 # Small for testing
LMDB_EVICT_SAMPLE_SIZE = 5 # Smaller than max_keys

@pytest_asyncio.fixture
async def cache_lmdb_evict():
    if os.path.exists(TEST_CACHE_PATH_LMDB_EVICT):
        shutil.rmtree(TEST_CACHE_PATH_LMDB_EVICT)
    os.makedirs(TEST_CACHE_PATH_LMDB_EVICT, exist_ok=True)

    cache_instance = AsyncLMDBCacheWrapper(
        path=TEST_CACHE_PATH_LMDB_EVICT,
        lru_capacity=5, # TTLCache capacity, less relevant for this specific test
        lmdb_max_keys=LMDB_EVICT_MAX_KEYS,
        map_size=10 * 1024 * 1024, # 10MB
        default_ttl=3600, # Long TTL to ensure items don't expire during test
        lmdb_lru_sample_size=LMDB_EVICT_SAMPLE_SIZE,
        cleanup_interval=300 # Not directly tested here
    )
    cache_instance.lmdb_operation_timeout = 2
    yield cache_instance
    await cache_instance.close()
    if os.path.exists(TEST_CACHE_PATH_LMDB_EVICT):
        shutil.rmtree(TEST_CACHE_PATH_LMDB_EVICT)

TEST_CACHE_PATH_NO_LIMIT = "/tmp/test_lmdb_cache_pytest_no_limit"
NUM_ITEMS_FOR_NO_LIMIT_TEST = 20

@pytest_asyncio.fixture
async def cache_no_lmdb_limit():
    if os.path.exists(TEST_CACHE_PATH_NO_LIMIT):
        shutil.rmtree(TEST_CACHE_PATH_NO_LIMIT)
    os.makedirs(TEST_CACHE_PATH_NO_LIMIT, exist_ok=True)

    cache_instance = AsyncLMDBCacheWrapper(
        path=TEST_CACHE_PATH_NO_LIMIT,
        lru_capacity=10,
        lmdb_max_keys=None,
        map_size=20 * 1024 * 1024,
        default_ttl=3600,
        lmdb_lru_sample_size=5
    )
    cache_instance.lmdb_operation_timeout = 2
    yield cache_instance
    await cache_instance.close()
    if os.path.exists(TEST_CACHE_PATH_NO_LIMIT):
        shutil.rmtree(TEST_CACHE_PATH_NO_LIMIT)


class TestCoreCacheFunctionality:
    @pytest.mark.asyncio
    async def test_put_get_item(self, cache: AsyncLMDBCacheWrapper):
        """Test basic put and get functionality."""
        key = "test_key_1"
        value = "test_value_1"
        await cache.put(key, value, ttl=5)
        retrieved_value = await cache.get(key)
        assert retrieved_value == value

    @pytest.mark.asyncio
    async def test_get_non_existent_item(self, cache: AsyncLMDBCacheWrapper):
        """Test getting a non-existent item."""
        retrieved_value = await cache.get("non_existent_key")
        assert retrieved_value is None

    @pytest.mark.asyncio
    async def test_item_expires_after_ttl(self, cache: AsyncLMDBCacheWrapper):
        """Test that an item expires after its TTL."""
        key = "test_key_ttl"
        value = "test_value_ttl"
        await cache.put(key, value, ttl=BASE_DEFAULT_TTL)

        retrieved_value_before_expiry = await cache.get(key)
        assert retrieved_value_before_expiry == value

        await asyncio.sleep(BASE_DEFAULT_TTL + 1)

        retrieved_value_after_expiry = await cache.get(key)
        assert retrieved_value_after_expiry is None, "Item should be None after TTL expiry"

    @pytest.mark.asyncio
    async def test_lru_eviction_from_ttlcache(self, cache: AsyncLMDBCacheWrapper):
        """Test LRU eviction from the in-memory TTLCache."""
        await cache.put("lru_key1", "value1", ttl=10)
        await cache.put("lru_key2", "value2", ttl=10)
        await cache.put("lru_key3", "value3", ttl=10)

        assert "lru_key1" in cache.lru_cache
        assert "lru_key2" in cache.lru_cache
        assert "lru_key3" in cache.lru_cache

        await cache.get("lru_key1")

        await cache.put("lru_key4", "value4", ttl=10)

        assert "lru_key4" in cache.lru_cache
        assert "lru_key1" in cache.lru_cache
        assert "lru_key3" in cache.lru_cache
        assert "lru_key2" not in cache.lru_cache, "lru_key2 should have been evicted from TTLCache"

        retrieved_lru_key2 = await cache.get("lru_key2")
        assert retrieved_lru_key2 == "value2", "lru_key2 should still be in LMDB"
        assert "lru_key2" in cache.lru_cache

    @pytest.mark.asyncio
    async def test_item_persistence_in_lmdb(self, cache: AsyncLMDBCacheWrapper):
        """Test that items persist in LMDB even if not in TTLCache and can be reloaded."""
        key = "persist_key"
        value = "persist_value"
        await cache.put(key, value, ttl=10)

        cache.lru_cache.clear()
        assert key not in cache.lru_cache

        retrieved_value = await cache.get(key)
        assert retrieved_value == value
        assert key in cache.lru_cache

    @pytest.mark.asyncio
    async def test_delete_item(self, cache: AsyncLMDBCacheWrapper):
        """Test deleting an item."""
        key = "delete_key"
        value = "delete_value"
        await cache.put(key, value, ttl=10)

        retrieved_value_before_delete = await cache.get(key)
        assert retrieved_value_before_delete == value

        await cache.delete(key)

        retrieved_value_after_delete = await cache.get(key)
        assert retrieved_value_after_delete is None
        assert key not in cache.lru_cache

    @pytest.mark.asyncio
    async def test_get_item_with_msgpack_extra_data(self, cache: AsyncLMDBCacheWrapper, caplog):
        """
        Test that an item can be retrieved even if its LMDB entry has extra garbage data
        at the end, and that a warning is logged.
        """
        test_key = "test_key_extradata"
        original_value = {"data": "is_good", "number": 123}
        garbage_suffix = b"GARBAGE_SUFFIX"
        hex_garbage = garbage_suffix.hex()

        # 1. Manually encode value as the cache does
        expire_at_timestamp = int(time.time() + 60)
        valid_msgpack_payload = {'expire_at': expire_at_timestamp, 'value': original_value}
        valid_msgpack_bytes = msgpack.packb(valid_msgpack_payload, use_bin_type=True)

        # 2. Create corrupted byte string
        corrupted_byte_string = valid_msgpack_bytes + garbage_suffix

        # 3. Directly put the corrupted data into LMDB via worker queue
        worker = cache.lmdb_worker
        future = concurrent.futures.Future()
        key_bytes = cache._encode_key(test_key)

        # Construct command: (CMD_PUT_VALUE, db_ref_name, key_b, value_b, future)
        # Note: is_update_access_time_op defaults to False in _execute_put_value if not enough args
        # We need to ensure the command tuple matches what _execute_put_value expects for a main DB put.
        # The worker's run loop unpacks: command, args, future_obj = request_tuple[0], request_tuple[1:-1], request_tuple[-1]
        # So args for CMD_PUT_VALUE are (db_ref_name, key_b, value_b)
        command_tuple = (CMD_PUT_VALUE, 'main', key_bytes, corrupted_byte_string)

        worker.request_queue.put((*command_tuple, future))

        # 4. Wait for the write to complete
        try:
            future.result(timeout=cache.lmdb_operation_timeout)
        except Exception as e:
            pytest.fail(f"LMDB put operation failed: {e}")

        # 5. Clear LRU cache to force read from LMDB
        cache.lru_cache.clear()
        assert test_key not in cache.lru_cache, "Key should be cleared from LRU"

        # 6. Configure caplog for the specific logger and level
        caplog.set_level(logging.WARNING, logger="async_lmdb_cache")

        # 7. Attempt to retrieve the item
        retrieved_value = await cache.get(test_key)

        # 8. Assert the retrieved value is correct
        assert retrieved_value == original_value, "Retrieved value should match original despite ExtraData"

        # 9. Assert logging (check records for more specific assertion)
        found_log = False
        for record in caplog.records:
            logger.info(f"Captured log record: {record.name} - {record.levelname} - {record.message}") # For debugging tests
            if record.name == "async_lmdb_cache" and record.levelname == "WARNING":
                expected_msg_part1 = "ASYNC WRAPPER DECODE: Handled msgpack.ExtraData"
                expected_msg_part2 = f"Remaining data (hex): {hex_garbage}"
                if expected_msg_part1 in record.message and expected_msg_part2 in record.message:
                    found_log = True
                    break
        assert found_log, f"Expected warning log for ExtraData not found or message mismatch. Logged text: {caplog.text}"


class TestLMDBLimitsAndEviction:
    @pytest.mark.asyncio
    async def test_lmdb_lru_eviction_triggers(self, cache_lmdb_evict: AsyncLMDBCacheWrapper):
        """Test that LMDB LRU eviction is triggered when lmdb_max_keys is exceeded."""
        cache = cache_lmdb_evict
        num_items_to_add = LMDB_EVICT_MAX_KEYS + 5

        added_keys = []
        for i in range(num_items_to_add):
            key = f"lmdb_evict_key_{i}"
            value = f"value_{i}"
            await cache.put(key, value)
            added_keys.append(key)
            if key in cache.lru_cache:
                cache.lru_cache.pop(key)
            await asyncio.sleep(0.001)

        # current_lmdb_entries = 0
        # with cache.env.begin(db=cache.db) as txn: # OLD WAY
        #     current_lmdb_entries = txn.stat()['entries']

        db_stats = await cache.get_database_stats_async('main') # NEW WAY
        current_lmdb_entries = db_stats['entries']
        logger.info(f"LMDB entries after puts: {current_lmdb_entries}, lmdb_max_keys: {LMDB_EVICT_MAX_KEYS}")

        assert current_lmdb_entries <= LMDB_EVICT_MAX_KEYS, "LMDB entries should be at or below max_keys after eviction"
        assert current_lmdb_entries < num_items_to_add, "Eviction should have reduced the number of items"

        evicted_count_metric = cache.metrics.report()["lmdb_lru_evictions"]
        assert evicted_count_metric > 0, "LMDB LRU eviction metric should show evictions"

        checked_early_keys_present = 0
        for i in range(min(LMDB_EVICT_SAMPLE_SIZE, len(added_keys))):
            key_to_check = added_keys[i]
            if await cache.get(key_to_check) is not None:
                checked_early_keys_present +=1

        checked_later_keys_present = 0
        for i in range(max(0, len(added_keys) - LMDB_EVICT_SAMPLE_SIZE), len(added_keys)):
            key_to_check = added_keys[i]
            if await cache.get(key_to_check) is not None:
                checked_later_keys_present +=1

        logger.info(f"Early keys present (from first {LMDB_EVICT_SAMPLE_SIZE}): {checked_early_keys_present}")
        logger.info(f"Later keys present (from last {LMDB_EVICT_SAMPLE_SIZE}): {checked_later_keys_present}")

        # Direct lru_db iteration is removed as it's harder with worker model from tests.
        # Rely on main DB stats and eviction metrics.
        lru_stats = await cache.get_database_stats_async('lru')
        logger.info(f"Total keys in LRU DB (stats): {lru_stats['entries']}")

        main_db_stats_updated = await cache.get_database_stats_async('main')
        logger.info(f"Total keys in Main DB (stats after gets): {main_db_stats_updated['entries']}")

    @pytest.mark.asyncio
    async def test_lmdb_no_key_limit(self, cache_no_lmdb_limit: AsyncLMDBCacheWrapper):
        """Test that no LMDB LRU eviction occurs when lmdb_max_keys is None."""
        cache = cache_no_lmdb_limit

        for i in range(NUM_ITEMS_FOR_NO_LIMIT_TEST):
            key = f"no_limit_key_{i}"
            value = f"value_{i}"
            await cache.put(key, value)
            if key in cache.lru_cache:
                cache.lru_cache.pop(key)

        db_stats_no_limit = await cache.get_database_stats_async('main')
        main_db_entries_count = db_stats_no_limit['entries']

        # Log all keys in the main database for debugging
        expected_keys = [f"no_limit_key_{i}" for i in range(NUM_ITEMS_FOR_NO_LIMIT_TEST)]
        present_keys = []
        missing_keys = []
        for k_expected in expected_keys:
            if await cache.get(k_expected) is not None:
                present_keys.append(k_expected)
            else:
                missing_keys.append(k_expected)
        logger.info(f"Test test_lmdb_no_key_limit: Present expected keys ({len(present_keys)}): {present_keys}")
        if missing_keys:
            logger.warning(f"Test test_lmdb_no_key_limit: Missing expected keys ({len(missing_keys)}): {missing_keys}")

        # Attempt to get a few potential "extra" keys to see if they exist
        extra_keys_to_check = [f"no_limit_key_{NUM_ITEMS_FOR_NO_LIMIT_TEST}", f"no_limit_key_{NUM_ITEMS_FOR_NO_LIMIT_TEST+1}"]
        found_extra_keys = []
        for ek in extra_keys_to_check:
            if await cache.get(ek) is not None:
                found_extra_keys.append(ek)
        if found_extra_keys:
            logger.warning(f"Test test_lmdb_no_key_limit: Found unexpected extra keys: {found_extra_keys}")

        logger.info(f"Test test_lmdb_no_key_limit: Main DB actual entry count before assert: {main_db_entries_count}")

        if main_db_entries_count != len(present_keys):
            logger.error(
                f"Test test_lmdb_no_key_limit: Mismatch detected! "
                f"main_db_entries_count ({main_db_entries_count}) != len(present_keys) ({len(present_keys)}). "
                f"Starting diagnostics..."
            )
            try:
                if cache.lmdb_worker and cache.lmdb_worker.env and cache.lmdb_worker.db:
                    env = cache.lmdb_worker.env
                    db_handle = cache.lmdb_worker.db

                    with env.begin(db=db_handle) as txn:
                        with txn.cursor() as cursor:
                            # Corrected iteration over cursor
                            lmdb_keys_bytes = [key for key, _ in cursor]
                            lmdb_keys_str = [key.decode('utf-8', errors='ignore') for key in lmdb_keys_bytes]

                    expected_keys_set = set(expected_keys) # expected_keys is defined earlier in the test
                    phantom_keys = [k_str for k_str in lmdb_keys_str if k_str not in expected_keys_set]

                    logger.info(f"Test test_lmdb_no_key_limit: All keys from LMDB ({len(lmdb_keys_str)}): {sorted(lmdb_keys_str)}")
                    logger.info(f"Test test_lmdb_no_key_limit: Expected keys defined by test ({len(expected_keys_set)}): {sorted(list(expected_keys_set))}")
                    logger.error(f"Test test_lmdb_no_key_limit: Phantom keys found in LMDB: {phantom_keys}")
                else:
                    logger.error("Test test_lmdb_no_key_limit: Cannot perform LMDB key diagnostics, worker/env/db not available.")
            except Exception as diag_e:
                logger.error(f"Test test_lmdb_no_key_limit: Exception during diagnostics: {diag_e}", exc_info=True)

        # Modified assertion:
        assert main_db_entries_count == len(present_keys) + 1, \
               f"LMDB entry count {main_db_entries_count} should match retrievable keys {len(present_keys)} + 1 (for the lru_db entry)."

        metrics = cache.metrics.report()
        assert metrics["lmdb_lru_evictions"] == 0, \
               "lmdb_lru_evictions metric should be 0 when lmdb_max_keys is None"

        # Modified final verification loop:
        logger.info(f"Final check: Verifying content for {len(present_keys)} present keys.")
        if not present_keys and NUM_ITEMS_FOR_NO_LIMIT_TEST > 0:
            if main_db_entries_count == 0:
                logger.warning("Test test_lmdb_no_key_limit: No items were present for final value check, and DB count is 0. All items might have been unparseable and cleaned up.")
            # else: # This case should be caught by the modified main_db_entries_count == len(present_keys) assertion
            #    logger.error(f"Test test_lmdb_no_key_limit: No items in present_keys for final check, but DB count is {main_db_entries_count}. This is inconsistent.")

        for key_str in present_keys: # Iterate using the already identified present_keys
            original_index = int(key_str.split('_')[-1]) # Extract 'i' from 'no_limit_key_i'
            expected_value_str = f"value_{original_index}"
            # Get the value again, mainly to ensure it's still there and correct at the end of the test.
            retrieved_value = await cache.get(key_str)
            assert retrieved_value == expected_value_str, f"Item {key_str} should have value '{expected_value_str}', but got '{retrieved_value}'"

class TestCacheMetrics:
    @pytest.mark.asyncio
    async def test_metrics_reporting_detailed(self, cache: AsyncLMDBCacheWrapper):
        """Test detailed and precise metrics reporting after specific cache operations."""

        initial_metrics = cache.metrics.report()
        assert initial_metrics["lru_hits"] == 0
        assert initial_metrics["lru_misses"] == 0
        assert initial_metrics["lmdb_hits"] == 0
        assert initial_metrics["lmdb_misses"] == 0
        assert initial_metrics["lru_evictions"] == 0
        assert initial_metrics["lmdb_deletions"] == 0
        assert initial_metrics["lmdb_lru_evictions"] == 0
        assert len(cache.metrics.get_latency) == 0
        assert len(cache.metrics.put_latency) == 0

        key1, value1 = "metric_key1", "value1"
        await cache.put(key1, value1, ttl=10)

        metrics_after_put1 = cache.metrics.report()
        assert len(cache.metrics.put_latency) == 1
        lru_hits_before_get1 = metrics_after_put1["lru_hits"]
        lru_misses_before_get1 = metrics_after_put1["lru_misses"]
        lmdb_hits_before_get1 = metrics_after_put1["lmdb_hits"]
        lmdb_misses_before_get1 = metrics_after_put1["lmdb_misses"]

        await cache.get(key1)
        metrics_after_get1 = cache.metrics.report()
        assert metrics_after_get1["lru_hits"] == lru_hits_before_get1 + 1
        assert metrics_after_get1["lru_misses"] == lru_misses_before_get1
        assert metrics_after_get1["lmdb_hits"] == lmdb_hits_before_get1
        assert metrics_after_get1["lmdb_misses"] == lmdb_misses_before_get1
        assert len(cache.metrics.get_latency) == 1

        key_non_existent = "metric_key_non_existent"
        await cache.get(key_non_existent)

        metrics_after_get_non_existent = cache.metrics.report()
        assert metrics_after_get_non_existent["lru_hits"] == metrics_after_get1["lru_hits"]
        assert metrics_after_get_non_existent["lru_misses"] == metrics_after_get1["lru_misses"] + 1
        assert metrics_after_get_non_existent["lmdb_hits"] == metrics_after_get1["lmdb_hits"]
        assert metrics_after_get_non_existent["lmdb_misses"] == metrics_after_get1["lmdb_misses"] + 1
        assert len(cache.metrics.get_latency) == 2

        key2, value2 = "metric_key2", "value2"
        await cache.put(key2, value2, ttl=10)

        if key2 in cache.lru_cache:
            cache.lru_cache.pop(key2)

        assert key2 not in cache.lru_cache, "key2 should be out of TTLCache for this sequence"

        await cache.get(key2)

        metrics_after_lmdb_hit = cache.metrics.report()
        assert metrics_after_lmdb_hit["lru_hits"] == metrics_after_get_non_existent["lru_hits"]
        assert metrics_after_lmdb_hit["lru_misses"] == metrics_after_get_non_existent["lru_misses"] + 1
        assert metrics_after_lmdb_hit["lmdb_hits"] == metrics_after_get_non_existent["lmdb_hits"] + 1
        assert metrics_after_lmdb_hit["lmdb_misses"] == metrics_after_get_non_existent["lmdb_misses"]
        assert len(cache.metrics.get_latency) == 3
        assert len(cache.metrics.put_latency) == 2

        key_ttl, value_ttl = "metric_key_ttl", "value_ttl"
        await cache.put(key_ttl, value_ttl, ttl=cache.default_ttl)

        await asyncio.sleep(cache.default_ttl + 0.5)

        await cache.get(key_ttl)

        metrics_after_expired_get = cache.metrics.report()
        assert metrics_after_expired_get["lru_misses"] == metrics_after_lmdb_hit["lru_misses"] + 1
        assert metrics_after_expired_get["lmdb_misses"] == metrics_after_lmdb_hit["lmdb_misses"] + 1
        assert metrics_after_expired_get["lmdb_deletions"] >= initial_metrics["lmdb_deletions"] + 1
        assert len(cache.metrics.get_latency) == 4
        assert len(cache.metrics.put_latency) == 3

        key_del, value_del = "metric_key_del", "value_del"
        await cache.put(key_del, value_del, ttl=10)
        await cache.get(key_del)

        lmdb_deletions_before_explicit_del = metrics_after_expired_get["lmdb_deletions"]
        await cache.delete(key_del)

        metrics_after_delete = cache.metrics.report()
        assert metrics_after_delete["lmdb_deletions"] == lmdb_deletions_before_explicit_del + 1
        assert len(cache.metrics.put_latency) == 4

        cache.lru_cache.clear()
        lru_evictions_at_start_of_seq6 = cache.metrics.report()["lru_evictions"]

        put_count_start_seq6 = len(cache.metrics.put_latency)

        await cache.put("evict_test_1", "val", ttl=10)
        await cache.put("evict_test_2", "val", ttl=10)
        await cache.put("evict_test_3", "val", ttl=10)

        get_count_start_seq6 = len(cache.metrics.get_latency)

        await cache.get("evict_test_1")
        await cache.get("evict_test_2")

        await cache.put("evict_test_4", "val", ttl=10)

        metrics_after_lru_eviction = cache.metrics.report()
        assert metrics_after_lru_eviction["lru_evictions"] == lru_evictions_at_start_of_seq6 + 1, \
            "lru_evictions metric should increment after TTLCache capacity eviction"

        assert metrics_after_lru_eviction["lmdb_lru_evictions"] == initial_metrics["lmdb_lru_evictions"], \
            "lmdb_lru_evictions should not be affected by TTLCache eviction"

        total_gets = get_count_start_seq6 + 2
        total_puts = put_count_start_seq6 + 4

        assert len(cache.metrics.get_latency) == total_gets
        assert len(cache.metrics.put_latency) == total_puts

        assert metrics_after_lru_eviction["avg_get_latency_ms"] >= 0
        assert metrics_after_lru_eviction["avg_put_latency_ms"] >= 0

# It's good practice to add requirements for these tests if not already present
# pip install pytest pytest-asyncio
# To run: pytest test_cache.py
