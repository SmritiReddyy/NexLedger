import redis.asyncio as aioredis
import json
import os
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")


class VelocityTracker:
    def __init__(self):
        self.redis = aioredis.from_url(REDIS_URL, decode_responses=True)

    async def get_velocity(self, account_id: str) -> dict:
        try:
            key_1h = f"velocity:1h:{account_id}"
            key_24h = f"velocity:24h:{account_id}"
            data_1h_raw = await self.redis.get(key_1h)
            data_24h_raw = await self.redis.get(key_24h)
            data_1h = json.loads(data_1h_raw) if data_1h_raw else {"count": 0, "amount": 0.0}
            data_24h = json.loads(data_24h_raw) if data_24h_raw else {"count": 0, "amount": 0.0}
            return {
                "count_1h": data_1h.get("count", 0),
                "amount_1h": data_1h.get("amount", 0.0),
                "count_24h": data_24h.get("count", 0),
                "amount_24h": data_24h.get("amount", 0.0),
            }
        except Exception as e:
            logger.warning("Redis velocity fetch failed: %s", e)
            return {"count_1h": 0, "amount_1h": 0.0, "count_24h": 0, "amount_24h": 0.0}

    async def record(self, account_id: str, amount: float):
        try:
            for window, ttl in [("1h", 3600), ("24h", 86400)]:
                key = f"velocity:{window}:{account_id}"
                raw = await self.redis.get(key)
                data = json.loads(raw) if raw else {"count": 0, "amount": 0.0}
                data["count"] += 1
                data["amount"] += amount
                await self.redis.setex(key, ttl, json.dumps(data))
        except Exception as e:
            logger.warning("Redis velocity record failed: %s", e)
