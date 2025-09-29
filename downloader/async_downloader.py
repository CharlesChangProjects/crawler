import aiohttp
import asyncio
import random
from typing import Dict, Any, Optional,List
from .user_agent_rotator import UserAgentRotator
from .proxy_manager import ProxyManager
from .rate_limiter import RateLimiter
from config.settings import config


class AsyncDownloader:
    def __init__(self):
        self.ua_rotator = UserAgentRotator()
        self.proxy_manager = ProxyManager()
        self.rate_limiter = RateLimiter()
        self.semaphore = asyncio.Semaphore(config.download.max_concurrent)
        self.session = None

    async def __aenter__(self):
        timeout = aiohttp.ClientTimeout(total=config.download.request_timeout)
        self.session = aiohttp.ClientSession(timeout=timeout)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def download(self, url: str, headers: Optional[Dict] = None,
                       proxy: Optional[str] = None, **kwargs) -> Dict[str, Any]:
        """异步下载页面"""
        async with self.semaphore:
            # 速率限制
            await self.rate_limiter.acquire()

            # 随机延迟
            if config.download.delay_range:
                delay = random.uniform(*config.download.delay_range)
                await asyncio.sleep(delay)

            # 准备请求头
            final_headers = {
                'User-Agent': self.ua_rotator.get_random_ua(),
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
            }
            if headers:
                final_headers.update(headers)

            # 重试机制
            for attempt in range(config.download.retry_times):
                try:
                    proxy_url = proxy or (await self.proxy_manager.get_proxy()
                                          if config.download.proxy_enabled else None)

                    async with self.session.get(
                            url,
                            headers=final_headers,
                            proxy=proxy_url,
                            **kwargs
                    ) as response:
                        content = await response.read()
                        return {
                            'url': url,
                            'content': content,
                            'status': response.status,
                            'headers': dict(response.headers),
                            'encoding': response.get_encoding(),
                            'cookies': dict(response.cookies),
                        }

                except Exception as e:
                    if attempt == config.download.retry_times - 1:
                        raise e
                    await asyncio.sleep(2 ** attempt)  # 指数退避

    async def download_batch(self, urls: List[str], **kwargs) -> List[Dict[str, Any]]:
        """批量下载"""
        tasks = [self.download(url, **kwargs) for url in urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # 处理结果
        processed_results = []
        for result in results:
            if isinstance(result, Exception):
                processed_results.append({'error': str(result)})
            else:
                processed_results.append(result)

        return processed_results