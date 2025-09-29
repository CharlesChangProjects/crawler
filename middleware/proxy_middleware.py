import asyncio
import time
from typing import Dict, Any, List, Optional
from config.settings import config


class ProxyMiddleware:
    """代理中间件 - 管理代理池和代理轮换"""

    def __init__(self):
        self.proxies = []
        self.bad_proxies = set()
        self.proxy_stats = {}
        self.current_proxy_index = 0
        self.last_proxy_rotation = time.time()
        self.proxy_rotation_interval = 300  # 5分钟

    async def process_request(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """处理请求前的代理逻辑"""
        if not config.download.proxy_enabled:
            return request

        # 检查是否需要轮换代理
        if self._should_rotate_proxies():
            await self.rotate_proxies()

        # 获取代理
        proxy = await self.get_proxy()
        if proxy:
            request['proxy'] = proxy
            self._update_proxy_stats(proxy, 'request')

        return request

    async def process_response(self, response: Dict[str, Any]) -> Dict[str, Any]:
        """处理响应后的代理逻辑"""
        proxy = response.get('request_info', {}).get('proxy')
        if proxy:
            status = response.get('status', 0)
            if 200 <= status < 400:
                self._update_proxy_stats(proxy, 'success')
            else:
                self._update_proxy_stats(proxy, 'failure')
                self.mark_bad_proxy(proxy)

        return response

    async def process_exception(self, exception: Exception, request: Dict[str, Any]):
        """处理异常时的代理逻辑"""
        proxy = request.get('proxy')
        if proxy:
            self._update_proxy_stats(proxy, 'error')
            self.mark_bad_proxy(proxy)

        return request

    async def get_proxy(self) -> Optional[str]:
        """获取可用的代理"""
        if not self.proxies:
            return None

        # 移除坏代理
        available_proxies = [p for p in self.proxies if p not in self.bad_proxies]
        if not available_proxies:
            # 如果没有可用代理，清空坏代理列表并重试
            self.bad_proxies.clear()
            available_proxies = self.proxies

        # 轮询选择代理
        if available_proxies:
            proxy = available_proxies[self.current_proxy_index % len(available_proxies)]
            self.current_proxy_index += 1
            return proxy

        return None

    def add_proxies(self, proxies: List[str]):
        """添加代理到池中"""
        for proxy in proxies:
            if proxy not in self.proxies:
                self.proxies.append(proxy)
                self.proxy_stats[proxy] = {
                    'requests': 0,
                    'success': 0,
                    'failures': 0,
                    'errors': 0,
                    'last_used': 0
                }

    def remove_proxy(self, proxy: str):
        """移除代理"""
        if proxy in self.proxies:
            self.proxies.remove(proxy)
        if proxy in self.proxy_stats:
            del self.proxy_stats[proxy]
        self.bad_proxies.discard(proxy)

    def mark_bad_proxy(self, proxy: str):
        """标记坏代理"""
        self.bad_proxies.add(proxy)
        # 设置超时，1小时后重新尝试
        asyncio.create_task(self._unmark_bad_proxy_after_timeout(proxy, 3600))

    async def _unmark_bad_proxy_after_timeout(self, proxy: str, timeout: int):
        """超时后取消标记坏代理"""
        await asyncio.sleep(timeout)
        self.bad_proxies.discard(proxy)

    async def rotate_proxies(self):
        """轮换代理"""
        self.last_proxy_rotation = time.time()
        # 这里可以添加从外部API获取新代理的逻辑
        logger.info("Rotating proxies")

    def _should_rotate_proxies(self) -> bool:
        """检查是否需要轮换代理"""
        current_time = time.time()
        return current_time - self.last_proxy_rotation > self.proxy_rotation_interval

    def _update_proxy_stats(self, proxy: str, event: str):
        """更新代理统计"""
        if proxy not in self.proxy_stats:
            self.proxy_stats[proxy] = {
                'requests': 0,
                'success': 0,
                'failures': 0,
                'errors': 0,
                'last_used': time.time()
            }

        stats = self.proxy_stats[proxy]
        stats['last_used'] = time.time()

        if event == 'request':
            stats['requests'] += 1
        elif event == 'success':
            stats['success'] += 1
        elif event == 'failure':
            stats['failures'] += 1
        elif event == 'error':
            stats['errors'] += 1

    def get_proxy_stats(self) -> Dict[str, Any]:
        """获取代理统计信息"""
        return {
            'total_proxies': len(self.proxies),
            'good_proxies': len(self.proxies) - len(self.bad_proxies),
            'bad_proxies': len(self.bad_proxies),
            'detailed_stats': self.proxy_stats
        }

    async def validate_proxies(self):
        """验证所有代理的可用性"""
        import aiohttp

        async def validate_proxy(proxy):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                            'http://httpbin.org/ip',
                            proxy=proxy,
                            timeout=10
                    ) as response:
                        if response.status == 200:
                            return True
            except:
                pass
            return False

        validation_tasks = [validate_proxy(proxy) for proxy in self.proxies]
        results = await asyncio.gather(*validation_tasks, return_exceptions=True)

        for i, is_valid in enumerate(results):
            if not is_valid:
                self.mark_bad_proxy(self.proxies[i])