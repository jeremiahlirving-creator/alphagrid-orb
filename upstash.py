"""
Upstash Redis REST client — uses POST body for reliable JSON storage.
"""
import os, aiohttp, json as _json

UPSTASH_URL   = os.getenv("UPSTASH_REDIS_REST_URL", "").rstrip("/")
UPSTASH_TOKEN = os.getenv("UPSTASH_REDIS_REST_TOKEN", "")

def _headers():
    return {
        "Authorization": f"Bearer {UPSTASH_TOKEN}",
        "Content-Type":  "application/json",
    }

async def redis_set_json(key: str, value: dict) -> bool:
    """Store a dict as JSON string using SET command via POST body."""
    if not UPSTASH_URL or not UPSTASH_TOKEN:
        return False
    try:
        serialized = _json.dumps(value)
        async with aiohttp.ClientSession() as s:
            async with s.post(
                UPSTASH_URL,
                headers=_headers(),
                json=["SET", key, serialized],
                timeout=aiohttp.ClientTimeout(total=5),
            ) as r:
                data = await r.json()
                return data.get("result") == "OK"
    except Exception as e:
        import logging
        logging.getLogger("upstash").warning(f"redis_set_json error: {e}")
        return False

async def redis_get_json(key: str) -> dict | None:
    """Retrieve a JSON dict using GET command via POST body."""
    if not UPSTASH_URL or not UPSTASH_TOKEN:
        return None
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(
                UPSTASH_URL,
                headers=_headers(),
                json=["GET", key],
                timeout=aiohttp.ClientTimeout(total=5),
            ) as r:
                data = await r.json()
                raw = data.get("result")
                if raw:
                    return _json.loads(raw)
    except Exception as e:
        import logging
        logging.getLogger("upstash").warning(f"redis_get_json error: {e}")
    return None
