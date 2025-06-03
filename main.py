from fastapi import FastAPI, HTTPException
import uvicorn
from typing import Optional
import asyncio # Required for asyncio.to_thread

# Assuming async_lmdb_cache.py is in the same directory
from async_lmdb_cache import AsyncLMDBCacheWrapper

app = FastAPI()

# Initialize the cache
# Ensure the path /tmp/my_lmdb_cache is appropriate for the execution environment
# or make it configurable.
# Adjusted parameters to match the implemented AsyncLMDBCacheWrapper
cache = AsyncLMDBCacheWrapper(
    path="/tmp/my_lmdb_cache", # This path should be writable by the application
    map_size=100 * 1024 * 1024  # Example map_size: 100MB
)

@app.on_event("startup")
async def startup_event():
    # Using start_expiry_check_task as defined in the cache class
    # The interval can be configured as needed.
    await cache.start_expiry_check_task(interval_seconds=600) # e.g., check every 10 minutes

@app.on_event("shutdown")
async def shutdown_event():
    await cache.close()

@app.get("/item/{key}")
async def get_item(key: str):
    val = await cache.get(key)
    if val is None:
        # Consider if this should distinguish between "not found" and "expired then deleted"
        raise HTTPException(status_code=404, detail="Item not found or expired")
    return {"key": key, "value": val}

@app.post("/item/{key}")
async def put_item(key: str, value: str, ttl: Optional[int] = None):
    # The cache's put method expects 'value: Any'.
    # Keeping 'value: str' here as per the example, but it can handle other pickleable types.
    await cache.put(key, value, ttl=ttl)
    # Constructing a response that reflects what was potentially stored.
    # If ttl is None, the cache might not apply a specific TTL (depends on its internal default if any)
    # Our current cache implementation doesn't have a "default_ttl" parameter at init,
    # so ttl is only applied if provided here.
    response_ttl = ttl if ttl is not None else "No TTL specified for this entry"
    return {"status": "success", "key": key, "value": value, "ttl_info": response_ttl}

@app.get("/metrics")
async def metrics():
    # Using get_metrics() as defined in the Metrics class via AsyncLMDBCacheWrapper
    return cache.get_metrics()

@app.delete("/clear_cache")
async def clear_cache_endpoint():
    try:
        # The clear method in AsyncLMDBCacheWrapper is async, so no need for asyncio.to_thread
        await cache.clear()
        return {"status": "success", "message": "Cache cleared"}
    except Exception as e:
        # Log the exception e for debugging purposes
        # import logging
        # logging.exception("Failed to clear cache")
        raise HTTPException(status_code=500, detail=f"Failed to clear cache: {str(e)}")


if __name__ == "__main__":
    # Ensure uvicorn is installed: pip install uvicorn fastapi
    # The path for LMDB should be writable.
    # For example, if running in Docker, ensure the /tmp directory or a mounted volume is writable.
    print("Starting FastAPI server with LMDB cache.")
    print(f"LMDB cache path: {cache.path}")
    print("Available endpoints:")
    print("  GET /item/{key}")
    print("  POST /item/{key} (body: {\"value\": \"your_value\", \"ttl\": optional_seconds})")
    print("  GET /metrics")
    print("  DELETE /clear_cache")
    print("Ensure the LMDB path '/tmp/my_lmdb_cache' is accessible and writable.")

    uvicorn.run(app, host="0.0.0.0", port=8000)
