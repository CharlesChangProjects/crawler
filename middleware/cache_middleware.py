import asyncio
import json
import hashlib
import time
from typing import Dict, Any, Optional
from config.redis_config import get_redis_connection
import logging
logger = logging.getLogger(__name__)

class CacheMiddleware:
    """缓存中间件 - 减少重复请求"""

    def __init__(self, cache_ttl: int = 3600):  # 默认1小时
        self.redis = get_redis_connection()
        self.cache_ttl = cache_ttl
        self.cache_hits = 0
        self.cache_misses = 0

    async def process_request(self, request: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """处理请求前的缓存逻辑"""
        cache_key = self._generate_cache_key(request)

        # 检查缓存
        cached_data = self.redis.get(cache_key)
        if cached_data:
            self.cache_hits += 1
            logger.debug(f"Cache hit for {request['url']}")
            return json.loads(cached_data)

        self.cache_misses += 1
        return None  # 继续正常请求

    async def process_response(self, response: Dict[str, Any]) -> Dict[str, Any]:
        """处理响应后的缓存逻辑"""
        # 只缓存成功的响应
        if response.get('status', 0) == 200:
            cache_key = self._generate_cache_key({'url': response['url']})
            cache_data = {
                'content': response['content'],
                'headers': response['headers'],
                'status': response['status'],
                'cached_at': time.time()
            }

            # 设置缓存
            self.redis.setex(cache_key, self.cache_ttl, json.dumps(cache_data))
            logger.debug(f"Cached response for {response['url']}")

        return response

    def _generate_cache_key(self, request: Dict[str, Any]) -> str:
        """生成缓存键"""
        url = request.get('url', '')
        method = request.get('method', 'GET').upper()

        # 对URL和请求方法进行哈希
        key_string = f"{method}:{url}"
        return f"cache:{hashlib.md5(key_string.encode()).hexdigest()}"

    def get_cache_stats(self) -> Dict[str, int]:
        """获取缓存统计"""
        return {
            'hits': self.cache_hits,
            'misses': self.cache_misses,
            'hit_ratio': self.cache_hits / (self.cache_hits + self.cache_misses) if (
                                                                                                self.cache_hits + self.cache_misses) > 0 else 0
        }

    async def clear_cache(self, pattern: str = "cache:*"):
        """清除缓存"""
        keys = self.redis.keys(pattern)
        if keys:
            self.redis.delete(*keys)
            logger.info(f"Cleared {len(keys)} cache entries")

    async def warmup_cache(self, urls: list, concurrency: int = 10):
        """预热缓存"""
        from downloader.async_downloader import AsyncDownloader

        async with AsyncDownloader() as downloader:
            semaphore = asyncio.Semaphore(concurrency)

            async def download_and_cache(url):
                async with semaphore:
                    try:
                        response = await downloader.download(url)
                        await self.process_response(response)
                        logger.debug(f"Warmed up cache for {url}")
                    except Exception as e:
                        logger.error(f"Failed to warm up cache for {url}: {e}")

            tasks = [download_and_cache(url) for url in urls]
            await asyncio.gather(*tasks, return_exceptions=True)