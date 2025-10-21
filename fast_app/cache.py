import redis
import json
from typing import Optional, Any
from fast_app.config import settings
from data_pipeline.base_logging import Logger

# Setup logger using base_logging module
logger = Logger("fast_app.cache")


class RedisCache:
    def __init__(self):
        logger.info(f"Initializing Redis cache connection to {settings.REDIS_HOST}:{settings.REDIS_PORT}")
        try:
            self.client = redis.Redis(
                host=settings.REDIS_HOST,
                port=settings.REDIS_PORT,
                db=settings.REDIS_DB,
                decode_responses=True
            )
            # Test the connection
            self.client.ping()
            logger.info("Redis cache connection established successfully")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {str(e)}")
            raise

    def get(self, key: str) -> Optional[Any]:
        try:
            value = self.client.get(key)
            if value:
                logger.debug(f"Cache HIT for key: {key}")
                return json.loads(value)
            else:
                logger.debug(f"Cache MISS for key: {key}")
                return None
        except Exception as e:
            logger.error(f"Error retrieving from cache (key: {key}): {str(e)}")
            return None

    def set(self, key: str, value: Any, ttl: int = None):
        ttl = ttl or settings.REDIS_TTL
        try:
            self.client.setex(key, ttl, json.dumps(value))
            logger.debug(f"Cache SET for key: {key} (TTL: {ttl}s)")
        except Exception as e:
            logger.error(f"Error setting cache (key: {key}): {str(e)}")

    def delete(self, key: str):
        try:
            result = self.client.delete(key)
            if result:
                logger.debug(f"Cache DELETE successful for key: {key}")
            else:
                logger.debug(f"Cache DELETE - key not found: {key}")
            return result
        except Exception as e:
            logger.error(f"Error deleting from cache (key: {key}): {str(e)}")
            return 0

    def clear_pattern(self, pattern: str):
        try:
            keys = self.client.keys(pattern)
            if keys:
                deleted_count = self.client.delete(*keys)
                logger.info(f"Cleared {deleted_count} cache entries matching pattern: {pattern}")
                return deleted_count
            else:
                logger.debug(f"No cache entries found matching pattern: {pattern}")
                return 0
        except Exception as e:
            logger.error(f"Error clearing cache pattern ({pattern}): {str(e)}")
            return 0

    def clear_all(self):
        try:
            self.client.flushdb()
            logger.info("All cache data cleared successfully")
        except Exception as e:
            logger.error(f"Error clearing all cache data: {str(e)}")
            raise
