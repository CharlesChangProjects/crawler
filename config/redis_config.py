# config/redis_config.py
import redis
from .settings import config

def get_redis_connection():
    """获取Redis连接"""
    return redis.Redis(
        host=config.redis.host,
        port=config.redis.port,
        db=config.redis.db,
        password=config.redis.password,
        decode_responses=True
    )