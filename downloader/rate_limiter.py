import asyncio
import time


class RateLimiter:
    def __init__(self, max_requests: int = 100, time_window: int = 60):
        self.max_requests = max_requests
        self.time_window = time_window
        self.requests = []
        self.lock = asyncio.Lock()

    async def acquire(self):
        """获取请求许可"""
        async with self.lock:
            now = time.time()

            # 移除过期的请求记录
            self.requests = [t for t in self.requests if now - t < self.time_window]

            # 检查是否超过限制
            if len(self.requests) >= self.max_requests:
                # 计算需要等待的时间
                oldest_request = self.requests[0]
                wait_time = self.time_window - (now - oldest_request)
                if wait_time > 0:
                    await asyncio.sleep(wait_time)
                    # 更新请求记录
                    self.requests = self.requests[1:]

            # 添加当前请求时间
            self.requests.append(now)