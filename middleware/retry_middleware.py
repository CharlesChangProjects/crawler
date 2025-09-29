import random
import asyncio
import time
from typing import Dict, Any

import logging
logger = logging.getLogger(__name__)

class RetryMiddleware:
    """重试中间件 - 处理请求失败的重试逻辑"""

    def __init__(self, max_retries: int = 3):
        self.max_retries = max_retries
        self.retry_queue = []
        self.retry_stats = {
            'total_retries': 0,
            'successful_retries': 0,
            'failed_retries': 0,
            'retry_attempts': {}
        }

    async def process_exception(self, exception: Exception, request: Dict[str, Any]) -> Dict[str, Any]:
        """处理异常时的重试逻辑"""
        url = request.get('url', '')
        retry_count = request.get('retry_count', 0)

        # 检查是否应该重试
        if retry_count < self.max_retries and self._should_retry(exception):
            retry_count += 1
            request['retry_count'] = retry_count

            # 计算重试延迟（指数退避）
            delay = self._calculate_retry_delay(retry_count)

            # 添加到重试队列
            self.retry_queue.append({
                'request': request,
                'retry_at': time.time() + delay,
                'attempt': retry_count
            })

            self.retry_stats['total_retries'] += 1
            self.retry_stats['retry_attempts'][url] = retry_count

            logger.info(f"Scheduling retry #{retry_count} for {url} in {delay:.2f}s")

        return request

    async def process_retry_queue(self):
        """处理重试队列"""
        current_time = time.time()
        retry_now = []

        # 找出需要立即重试的请求
        remaining = []
        for item in self.retry_queue:
            if item['retry_at'] <= current_time:
                retry_now.append(item)
            else:
                remaining.append(item)

        self.retry_queue = remaining

        # 执行重试
        if retry_now:
            logger.info(f"Processing {len(retry_now)} retries")

            for item in retry_now:
                try:
                    # 这里应该调用下载器重新执行请求
                    # 实际实现中需要注入下载器实例
                    # await self.downloader.download(item['request'])
                    self.retry_stats['successful_retries'] += 1
                    logger.info(f"Retry successful for {item['request']['url']}")
                except Exception as e:
                    self.retry_stats['failed_retries'] += 1
                    logger.error(f"Retry failed for {item['request']['url']}: {e}")
                    # 可以继续重试或者放弃
                    if item['attempt'] < self.max_retries:
                        await self.process_exception(e, item['request'])

    def _should_retry(self, exception: Exception) -> bool:
        """判断是否应该重试"""
        error_msg = str(exception).lower()

        # 应该重试的错误
        retryable_errors = [
            'timeout', 'connection', 'network', 'temporary',
            'busy', 'overload', 'rate limit', '429', '503'
        ]

        # 不应该重试的错误
        non_retryable_errors = [
            '404', 'not found', '403', 'forbidden', '401', 'unauthorized',
            '400', 'bad request', 'invalid'
        ]

        if any(error in error_msg for error in non_retryable_errors):
            return False

        return any(error in error_msg for error in retryable_errors)

    def _calculate_retry_delay(self, attempt: int) -> float:
        """计算重试延迟（指数退避）"""
        base_delay = 2.0  # 基础延迟2秒
        max_delay = 60.0  # 最大延迟60秒
        delay = min(base_delay * (2 ** (attempt - 1)), max_delay)

        # 添加随机抖动
        jitter = random.uniform(0.1, 0.5)
        return delay + jitter

    def get_retry_stats(self) -> Dict[str, Any]:
        """获取重试统计"""
        return self.retry_stats

    def clear_retry_queue(self):
        """清空重试队列"""
        self.retry_queue.clear()
        logger.info("Cleared retry queue")

    async def schedule_periodic_retry(self, interval: int = 30):
        """定期处理重试队列"""
        while True:
            try:
                await self.process_retry_queue()
                await asyncio.sleep(interval)
            except Exception as e:
                logger.error(f"Error in periodic retry processing: {e}")
                await asyncio.sleep(5)