import aiohttp
from typing import List, Optional


class ProxyManager:
    def __init__(self, proxy_list: List[str] = None):
        self.proxies = proxy_list or []
        self.current_index = 0
        self.bad_proxies = set()

    async def get_proxy(self) -> Optional[str]:
        """获取代理"""
        if not self.proxies:
            return None

        # 简单轮询
        proxy = self.proxies[self.current_index]
        self.current_index = (self.current_index + 1) % len(self.proxies)

        # 检查代理是否可用
        if proxy in self.bad_proxies:
            return await self.get_proxy()  # 递归获取下一个

        return proxy

    async def validate_proxy(self, proxy: str) -> bool:
        """验证代理是否可用"""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                        "http://httpbin.org/ip",
                        proxy=proxy,
                        timeout=10
                ) as response:
                    return response.status == 200
        except:
            return False

    def add_proxy(self, proxy: str):
        """添加代理"""
        if proxy not in self.proxies:
            self.proxies.append(proxy)

    def remove_proxy(self, proxy: str):
        """移除代理"""
        if proxy in self.proxies:
            self.proxies.remove(proxy)

    def mark_bad_proxy(self, proxy: str):
        """标记坏代理"""
        self.bad_proxies.add(proxy)
        # 可以设置超时，一段时间后重新尝试