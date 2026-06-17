"""
Upstash Redis REST client — pipeline endpoint, robust error logging.
"""
import os, aiohttp, json as _json, logging

logger = logging.getLogger("alphagrid")

UPSTASH_URL   = os.getenv("UPSTASH_REDIS_REST_URL", "").rstrip("/")
UPSTASH_TOKEN = os.getenv("UPSTASH_REDIS_REST_TOKEN", "")

async def redis_set_json(key: str, value: dict) -> bool:
    if not UPSTASH_URL or not UPSTASH_TOKEN:
        logger.warning(f"⚠️ Upstash not configured (URL={bool(UPSTASH_URL)} TOKEN={bool(UPSTASH_TOKEN)})")
        return False
    try:
        serialized = _json.dumps(value)
        async with aiohttp.ClientSession() as s:
            async with s.post(
                f"{UPSTASH_URL}/pipeline",
                headers={"Authorization": f"Bearer {UPSTASH_TOKEN}", "Content-Type": "application/json"},
                json=[["SET", key, serialized]],
                timeout=aiohttp.ClientTimeout(total=5),
            ) as r:
                text = await r.text()
                logger.info(f"📦 Upstash SET {key} → HTTP {r.status} | {text[:100]}")
                return r.status == 200
    except Exception as e:
        logger.error(f"❌ Upstash SET {key} failed: {e}")
        return False

async def redis_get_json(key: str) -> dict | None:
    if not UPSTASH_URL or not UPSTASH_TOKEN:
        logger.warning(f"⚠️ Upstash not configured — cannot GET {key}")
        return None
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(
                f"{UPSTASH_URL}/pipeline",
                headers={"Authorization": f"Bearer {UPSTASH_TOKEN}", "Content-Type": "application/json"},
                json=[["GET", key]],
                timeout=aiohttp.ClientTimeout(total=5),
            ) as r:
                text = await r.text()
                logger.info(f"📦 Upstash GET {key} → HTTP {r.status} | {text[:100]}")
                if r.status == 200:
                    data = _json.loads(text)
                    if isinstance(data, list) and data:
                        raw = data[0].get("result")
                        if raw:
                            return _json.loads(raw)
    except Exception as e:
        logger.error(f"❌ Upstash GET {key} failed: {e}")
    return None
