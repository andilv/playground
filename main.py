from fastapi import FastAPI, HTTPException, Body
import uvicorn
from typing import Optional, Any
import os
import shutil # For cleaning up cache directory

# Assuming async_lmdb_cache.py is in the same directory
from async_lmdb_cache import AsyncLMDBCacheWrapper # This now refers to the new advanced version

app = FastAPI()

# Configuration for the cache
CACHE_PATH = "/tmp/my_advanced_lmdb_async_cache"
LRU_CAPACITY = 5000
LMDB_MAX_KEYS = 1000000 # Can be None for no limit
MAP_SIZE = 10**9 # 1 GB, adjust as needed
DEFAULT_TTL = 3600 # 1 hour
CLEANUP_INTERVAL = 600 # 10 minutes
LMDB_LRU_SAMPLE_SIZE = 1000 # Smaller sample for example

# Initialize the new AsyncLMDBCacheWrapper
# Note: The new wrapper uses different parameters
async_cache = AsyncLMDBCacheWrapper(
    path=CACHE_PATH,
    lru_capacity=LRU_CAPACITY,
    lmdb_max_keys=LMDB_MAX_KEYS,
    map_size=MAP_SIZE,
    default_ttl=DEFAULT_TTL,
    cleanup_interval=CLEANUP_INTERVAL,
    lmdb_lru_sample_size=LMDB_LRU_SAMPLE_SIZE
)

@app.on_event("startup")
async def startup_event():
    # Clean up previous cache if it exists (optional, for clean testing)
    if os.path.exists(CACHE_PATH):
        shutil.rmtree(CACHE_PATH)
        print(f"Cleaned up existing async cache at {CACHE_PATH}")
    os.makedirs(CACHE_PATH, exist_ok=True) # Ensure directory exists

    # Start the background cleanup task using the new method name
    await async_cache.start_background_cleanup()
    print("Async cache background cleanup started.")

@app.on_event("shutdown")
async def shutdown_event():
    await async_cache.close()
    print("Async cache closed.")
    # Optional: Clean up cache directory on shutdown
    # if os.path.exists(CACHE_PATH):
    #     shutil.rmtree(CACHE_PATH)
    #     print(f"Cleaned up async cache at {CACHE_PATH} on shutdown.")


@app.get("/item/{key}")
async def get_item(key: str):
    val = await async_cache.get(key)
    if val is None:
        raise HTTPException(status_code=404, detail="Item not found or expired")
    return {"key": key, "value": val}

# Make sure the input for 'value' can be Any type that msgpack can serialize
# Using Body embed parameter to expect a JSON object like {"value": "some_value", "ttl": 300}
@app.post("/item/{key}")
async def put_item(key: str, payload: dict = Body(...)):
    value: Any = payload.get("value")
    ttl: Optional[int] = payload.get("ttl")

    if value is None:
        raise HTTPException(status_code=400, detail="Value not provided in payload")

    await async_cache.put(key, value, ttl=ttl)
    return {"status": "success", "key": key, "value": value, "ttl_applied_seconds": ttl if ttl is not None else async_cache.default_ttl}

@app.get("/metrics")
async def metrics():
    # The new Metrics class has a 'report' method
    return async_cache.metrics.report()

# The /clear_cache endpoint is removed as per plan.
# If needed, a new 'clear' or 'delete_all' method could be added to BaseLMDBCacheWrapper
# and exposed here. For now, it's removed.

# Example of how to add a delete endpoint for a specific key
@app.delete("/item/{key}")
async def delete_item(key: str):
    try:
        await async_cache.delete(key) # Assuming new cache has an async delete method
        return {"status": "success", "message": f"Item {key} deleted."}
    except Exception as e:
        # Handle cases like key not found if necessary, or log error
        raise HTTPException(status_code=500, detail=f"Error deleting item {key}: {str(e)}")


if __name__ == "__main__":
    print("Starting FastAPI server with Advanced LMDB cache.")
    print(f"LMDB cache path: {async_cache.env.path().decode() if async_cache.env else CACHE_PATH}") # Accessing path after env is initialized
    print(f"Default TTL: {async_cache.default_ttl}s, LRU Capacity: {async_cache.lru_cache.maxsize}")
    print("Available endpoints:")
    print(f"  GET /item/{{key}}")
    print(f"  POST /item/{{key}} (JSON body: {{"value": "your_value", "ttl": {optional_seconds}}})")
    print(f"  DELETE /item/{{key}}")
    print(f"  GET /metrics")
    print(f"Ensure the LMDB path '{CACHE_PATH}' is accessible and writable.")

    # Create cache directory if it doesn't exist, before uvicorn starts and cache is fully initialized
    if not os.path.exists(CACHE_PATH):
        os.makedirs(CACHE_PATH)
        print(f"Created cache directory: {CACHE_PATH}")

    uvicorn.run(app, host="0.0.0.0", port=8000)
