import asyncio
import os
import shutil
import time
import pytest
import pytest_asyncio # For async fixtures

from async_lmdb_cache import AsyncLMDBCacheWrapper, Metrics # Assuming this is the correct import path

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
    os.makedirs(TEST_CACHE_PATH, exist_ok=True)

    cache_instance = AsyncLMDBCacheWrapper(
        path=TEST_CACHE_PATH,
        lru_capacity=BASE_LRU_CAPACITY,
        default_ttl=BASE_DEFAULT_TTL,
        lmdb_max_keys=BASE_LMDB_MAX_KEYS,
        map_size=BASE_MAP_SIZE,
        cleanup_interval=1 # Short interval for testing, though not directly tested here
    )
    await cache_instance.start_background_cleanup()
    yield cache_instance # Test runs here
    await cache_instance.close()
    if os.path.exists(TEST_CACHE_PATH):
        shutil.rmtree(TEST_CACHE_PATH)

@pytest.mark.asyncio
async def test_put_get_item(cache: AsyncLMDBCacheWrapper):
    """Test basic put and get functionality."""
    key = "test_key_1"
    value = "test_value_1"
    await cache.put(key, value, ttl=5) # Longer TTL for this test
    retrieved_value = await cache.get(key)
    assert retrieved_value == value

@pytest.mark.asyncio
async def test_get_non_existent_item(cache: AsyncLMDBCacheWrapper):
    """Test getting a non-existent item."""
    retrieved_value = await cache.get("non_existent_key")
    assert retrieved_value is None

@pytest.mark.asyncio
async def test_item_expires_after_ttl(cache: AsyncLMDBCacheWrapper):
    """Test that an item expires after its TTL."""
    key = "test_key_ttl"
    value = "test_value_ttl"
    await cache.put(key, value, ttl=BASE_DEFAULT_TTL) # Use fixture's default TTL (1s)

    # Item should be present immediately
    retrieved_value_before_expiry = await cache.get(key)
    assert retrieved_value_before_expiry == value

    await asyncio.sleep(BASE_DEFAULT_TTL + 1) # Wait for TTL to expire + buffer

    retrieved_value_after_expiry = await cache.get(key)
    assert retrieved_value_after_expiry is None, "Item should be None after TTL expiry"

@pytest.mark.asyncio
async def test_lru_eviction_from_ttlcache(cache: AsyncLMDBCacheWrapper):
    """Test LRU eviction from the in-memory TTLCache."""
    # Fill the TTLCache to its capacity (BASE_LRU_CAPACITY = 3)
    await cache.put("lru_key1", "value1", ttl=10)
    await cache.put("lru_key2", "value2", ttl=10)
    await cache.put("lru_key3", "value3", ttl=10)

    # At this point, lru_key1, lru_key2, lru_key3 are in TTLCache
    assert "lru_key1" in cache.lru_cache
    assert "lru_key2" in cache.lru_cache
    assert "lru_key3" in cache.lru_cache

    # Access lru_key1 to make it most recently used among the first three
    await cache.get("lru_key1")

    # Add another item, which should evict the least recently used item from TTLCache.
    # Current order (MRU to LRU): lru_key1, lru_key3, lru_key2
    # So, lru_key2 should be evicted from TTLCache.
    await cache.put("lru_key4", "value4", ttl=10)

    # Check TTLCache contents
    assert "lru_key4" in cache.lru_cache
    assert "lru_key1" in cache.lru_cache # Was accessed
    assert "lru_key3" in cache.lru_cache
    assert "lru_key2" not in cache.lru_cache, "lru_key2 should have been evicted from TTLCache"

    # Verify that lru_key2 is still retrievable from LMDB
    retrieved_lru_key2 = await cache.get("lru_key2")
    assert retrieved_lru_key2 == "value2", "lru_key2 should still be in LMDB"
    # And now lru_key2 should be back in TTLCache
    assert "lru_key2" in cache.lru_cache


@pytest.mark.asyncio
async def test_item_persistence_in_lmdb(cache: AsyncLMDBCacheWrapper):
    """Test that items persist in LMDB even if not in TTLCache and can be reloaded."""
    key = "persist_key"
    value = "persist_value"
    await cache.put(key, value, ttl=10)

    # Simulate TTLCache eviction by clearing it (or just ensure key is not there)
    # For this test, let's assume it got evicted or we are checking after a restart (conceptually)
    cache.lru_cache.clear()
    assert key not in cache.lru_cache

    # Retrieve the item. It should be fetched from LMDB.
    retrieved_value = await cache.get(key)
    assert retrieved_value == value
    # After retrieval, it should be back in TTLCache
    assert key in cache.lru_cache

@pytest.mark.asyncio
async def test_delete_item(cache: AsyncLMDBCacheWrapper):
    """Test deleting an item."""
    key = "delete_key"
    value = "delete_value"
    await cache.put(key, value, ttl=10)

    retrieved_value_before_delete = await cache.get(key)
    assert retrieved_value_before_delete == value

    await cache.delete(key)

    retrieved_value_after_delete = await cache.get(key)
    assert retrieved_value_after_delete is None
    assert key not in cache.lru_cache # Should also be removed from TTLCache

@pytest.mark.asyncio
async def test_metrics_reporting(cache: AsyncLMDBCacheWrapper):
    """Test basic metrics reporting."""
    key1 = "metrics_key1"
    value1 = "metrics_value1"
    key2 = "metrics_key2" # Will be a miss then a hit

    # Initial state
    initial_metrics = cache.metrics.report()
    assert initial_metrics["lru_hits"] == 0
    assert initial_metrics["lmdb_hits"] == 0
    assert initial_metrics["lru_misses"] == 0
    assert initial_metrics["lmdb_misses"] == 0

    # Put and Get (LRU hit for TTLCache, LMDB access time update)
    await cache.put(key1, value1, ttl=5) # Put into LMDB and TTLCache
    await cache.get(key1) # Get from TTLCache

    # Get non-existent (LRU miss, LMDB miss)
    await cache.get(key2)

    # Put second item, then get it (loaded into TTLCache from LMDB after initial miss)
    await cache.put(key2, "value2", ttl=5)
    await cache.get(key2) # Get from TTLCache (was an LMDB hit that populated TTLCache)

    final_metrics = cache.metrics.report()

    # Based on the operations:
    # put(key1): no hit/miss change for 'get' metrics
    # get(key1): lru_hit for key1
    # get(key2): lru_miss for key2, lmdb_miss for key2
    # put(key2): no hit/miss change
    # get(key2): lru_hit for key2 (it was put into lru_cache during the previous put)

    # Let's re-evaluate the sequence for metrics:
    # 1. await cache.put(key1, value1, ttl=5)
    #    - TTLCache: key1 is added.
    #    - LMDB: key1 is added.
    # 2. await cache.get(key1)
    #    - TTLCache: Hit for key1. metrics.lru_hits = 1.
    # 3. await cache.get(key2)
    #    - TTLCache: Miss for key2. metrics.lru_misses = 1.
    #    - LMDB: Miss for key2. metrics.lmdb_misses = 1.
    # 4. await cache.put(key2, "value2", ttl=5)
    #    - TTLCache: key2 is added.
    #    - LMDB: key2 is added.
    # 5. await cache.get(key2)
    #    - TTLCache: Hit for key2. metrics.lru_hits = 2.

    assert final_metrics["lru_hits"] >= 2 # key1, key2
    assert final_metrics["lru_misses"] >= 1 # key2 initial miss
    assert final_metrics["lmdb_misses"] >= 1 # key2 initial miss from lmdb
    # lmdb_hits occur when an item is found in LMDB after a TTLCache miss.
    # In the above sequence, get(key2) is an lru_miss and lmdb_miss.
    # Then put(key2) adds it.
    # Then get(key2) is an lru_hit.
    # To test lmdb_hit:
    #   put("lmdb_hit_key", "v")
    #   cache.lru_cache.pop("lmdb_hit_key") # Simulate eviction
    #   await cache.get("lmdb_hit_key") -> This would be lru_miss, lmdb_hit

    # Test LMDB hit explicitly
    key_lmdb_hit = "key_for_lmdb_hit"
    await cache.put(key_lmdb_hit, "value_lmdb_hit", ttl=5)
    # Ensure it's out of lru_cache to force LMDB lookup
    popped = cache.lru_cache.pop(key_lmdb_hit, None)
    assert popped is not None, "Key should have been in lru_cache to pop"
    assert key_lmdb_hit not in cache.lru_cache

    await cache.get(key_lmdb_hit) # This should be an LRU miss and an LMDB hit

    final_metrics_after_lmdb_hit_test = cache.metrics.report()
    assert final_metrics_after_lmdb_hit_test["lmdb_hits"] >= 1
    assert final_metrics_after_lmdb_hit_test["avg_get_latency_ms"] > 0 or len(cache.metrics.get_latency) == 0 # check avg or raw
    assert final_metrics_after_lmdb_hit_test["avg_put_latency_ms"] > 0 or len(cache.metrics.put_latency) == 0 # check avg or raw

# It's good practice to add requirements for these tests if not already present
# pip install pytest pytest-asyncio
# To run: pytest test_cache.py
