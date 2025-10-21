import redis
import json
from typing import Optional, Any
from fast_app.config import settings


class RedisCache:
    def __init__(self):
        self.client = redis.Redis(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            db=settings.REDIS_DB,
            decode_responses=True
        )

    def get(self, key: str) -> Optional[Any]:
        value = self.client.get(key)
        return json.loads(value) if value else None

    def set(self, key: str, value: Any, ttl: int = None):
        ttl = ttl or settings.REDIS_TTL
        self.client.setex(key, ttl, json.dumps(value))

    def delete(self, key: str):
        self.client.delete(key)

    def clear_pattern(self, pattern: str):
        keys = self.client.keys(pattern)
        if keys:
            self.client.delete(*keys)

    def clear_all(self):
        self.client.flushdb()
