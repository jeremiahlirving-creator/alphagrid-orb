"""
Upstash Redis REST client — zero dependencies, pure aiohttp.
Used by AlphaGrid bots to persist state across Railway redeploys.
"""
import os, aiohttp, json as _json

UPSTASH_URL   = os.getenv("UPSTASH_REDIS_REST_URL", "")
UPSTASH_TOKEN = os.getenv("UPSTASH_REDIS_REST_TOKEN", "")

async def redis_set(key: str, value: str) -> bool:
    """SET key value in Upstash Redis."""
    if not UPSTASH_URL or not UPSTASH_TOKEN:
        return False
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(
                f"{UPSTASH_URL}/set/{key}/{value}",
                headers={"Authorization": f"Bearer {UPSTASH_TOKEN}"},
                timeout=aiohttp.ClientTimeout(total=3),
            ) as r:
                return r.status == 200
    except Exception:
        return False

async def redis_get(key: str) -> str | None:
    """GET key from Upstash Redis. Returns None if missing."""
    if not UPSTASH_URL or not UPSTASH_TOKEN:
        return None
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(
                f"{UPSTASH_URL}/get/{key}",
                headers={"Authorization": f"Bearer {UPSTASH_TOKEN}"},
                timeout=aiohttp.ClientTimeout(total=3),
            ) as r:
                if r.status == 200:
                    data = await r.json()
                    return data.get("result")
    except Exception:
        pass
    return None

async def redis_set_json(key: str, value: dict) -> bool:
    """Store a dict as JSON string."""
    encoded = _json.dumps(value).replace("/", "%2F")
    return await redis_set(key, encoded)

async def redis_get_json(key: str) -> dict | None:
    """Retrieve a JSON dict."""
    raw = await redis_get(key)
    if raw:
        try:
            return _json.loads(raw)
        except Exception:
            pass
    return None
