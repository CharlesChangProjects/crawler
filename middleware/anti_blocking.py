import asyncio
import random
import time
import logging
from typing import Dict, Any
from urllib.parse import urlparse


logger = logging.getLogger(__name__)


class AntiBlockingMiddleware:
    """反屏蔽中间件 - 防止被网站封禁"""

    def __init__(self):
        self.request_timestamps = {}
        self.domain_delays = {}
        self.blocked_domains = set()
        self.blocked_until = {}

        # 人类行为模拟参数
        self.mouse_movements = []
        self.scroll_actions = []
        self.click_patterns = []

    async def process_request(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """处理请求前的反屏蔽逻辑"""
        url = request.get('url', '')
        domain = urlparse(url).netloc

        # 检查域名是否被暂时封禁
        if domain in self.blocked_until:
            if time.time() < self.blocked_until[domain]:
                logger.warning(f"Domain {domain} is blocked until {self.blocked_until[domain]}")
                raise Exception(f"Domain {domain} is temporarily blocked")
            else:
                self.blocked_until.pop(domain, None)

        # 应用域名特定的延迟
        if domain in self.domain_delays:
            delay = self.domain_delays[domain]
            logger.debug(f"Applying domain-specific delay {delay}s for {domain}")
            await asyncio.sleep(delay)

        # 随机延迟（避免规律性请求）
        jitter = random.uniform(0.1, 0.5)
        await asyncio.sleep(jitter)

        # 更新请求时间记录
        current_time = time.time()
        if domain not in self.request_timestamps:
            self.request_timestamps[domain] = []

        self.request_timestamps[domain].append(current_time)
        # 只保留最近1分钟的记录
        self.request_timestamps[domain] = [
            ts for ts in self.request_timestamps[domain]
            if current_time - ts < 60
        ]

        return request

    async def process_response(self, response: Dict[str, Any]) -> Dict[str, Any]:
        """处理响应后的反屏蔽逻辑"""
        url = response.get('url', '')
        status = response.get('status', 0)
        domain = urlparse(url).netloc

        # 检查是否被屏蔽
        if self._is_blocked_response(response):
            logger.warning(f"Detected blocking from {domain}")
            self._handle_blocking(domain)
            raise Exception(f"Blocked by {domain}")

        # 根据响应状态调整策略
        if status == 429:  # Too Many Requests
            self._handle_rate_limit(domain)
            raise Exception(f"Rate limited by {domain}")

        return response

    async def process_exception(self, exception: Exception, request: Dict[str, Any]):
        """处理异常时的反屏蔽逻辑"""
        url = request.get('url', '')
        domain = urlparse(url).netloc

        if "blocked" in str(exception).lower() or "429" in str(exception):
            self._handle_blocking(domain)

        return request

    def _is_blocked_response(self, response: Dict[str, Any]) -> bool:
        """判断响应是否表明被屏蔽"""
        status = response.get('status', 0)
        content = response.get('content', b'')
        headers = response.get('headers', {})

        # 状态码检查
        if status in [403, 503, 999]:
            return True

        # 内容检查
        if content:
            content_str = content.decode('utf-8', errors='ignore').lower()
            blocked_indicators = [
                'access denied', 'blocked', 'robot', 'captcha',
                'cloudflare', 'distil', 'imperva', 'incapsula'
            ]
            if any(indicator in content_str for indicator in blocked_indicators):
                return True

        # Header检查
        server = headers.get('server', '').lower()
        if any(proxy in server for proxy in ['cloudflare', 'distil', 'imperva']):
            return True

        return False

    def _handle_blocking(self, domain: str):
        """处理被屏蔽的情况"""
        block_duration = random.randint(300, 1800)  # 5-30分钟
        self.blocked_until[domain] = time.time() + block_duration
        self.blocked_domains.add(domain)

        # 增加该域名的延迟
        self.domain_delays[domain] = random.uniform(2.0, 5.0)

        logger.warning(f"Domain {domain} blocked for {block_duration} seconds")

    def _handle_rate_limit(self, domain: str):
        """处理速率限制"""
        # 增加延迟时间
        current_delay = self.domain_delays.get(domain, 1.0)
        new_delay = min(current_delay * 2, 10.0)  # 最大10秒延迟
        self.domain_delays[domain] = new_delay

        # 短暂暂停
        pause_time = random.randint(60, 300)  # 1-5分钟
        self.blocked_until[domain] = time.time() + pause_time

        logger.warning(f"Rate limited by {domain}, increasing delay to {new_delay}s")

    def get_domain_stats(self, domain: str) -> Dict[str, Any]:
        """获取域名统计信息"""
        timestamps = self.request_timestamps.get(domain, [])
        current_time = time.time()

        return {
            'requests_last_minute': len([ts for ts in timestamps if current_time - ts < 60]),
            'requests_last_5min': len([ts for ts in timestamps if current_time - ts < 300]),
            'is_blocked': domain in self.blocked_until and time.time() < self.blocked_until[domain],
            'current_delay': self.domain_delays.get(domain, 0),
            'blocked_until': self.blocked_until.get(domain)
        }

    def reset_domain(self, domain: str):
        """重置域名的屏蔽状态"""
        self.blocked_domains.discard(domain)
        self.blocked_until.pop(domain, None)
        self.domain_delays.pop(domain, None)
        self.request_timestamps.pop(domain, None)
        logger.info(f"Reset anti-blocking for domain: {domain}")