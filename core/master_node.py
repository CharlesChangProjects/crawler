import asyncio
import logging
import time
from typing import List
from .distributed_manager import DistributedManager
from utils.url_manager import URLManager

logger = logging.getLogger(__name__)


class CrawlerMaster:
    def __init__(self):
        self.distributed_manager = DistributedManager()
        self.url_manager = URLManager()
        self.is_running = False
        self.stats = {
            'total_tasks': 0,
            'completed_tasks': 0,
            'failed_tasks': 0,
            'start_time': time.time()
        }

    async def add_seed_urls(self, urls: List[str], priority: int = 10):
        """添加种子URL"""
        added_count = 0
        for url in urls:
            if not self.url_manager.is_visited(url):
                await self.distributed_manager.push_task({
                    'url': url,
                    'priority': priority,
                    'metadata': {'type': 'seed'},
                    'timestamp': time.time()
                })
                added_count += 1
                logger.info(f"Added seed URL: {url}")

        logger.info(f"Added {added_count} seed URLs")

    async def monitor_tasks(self):
        """监控任务状态"""
        while self.is_running:
            try:
                # 处理完成的任务
                result = await self.distributed_manager.pop_result()
                if result:
                    if result.get('success'):
                        self.stats['completed_tasks'] += 1
                    else:
                        self.stats['failed_tasks'] += 1

                # 更新统计信息
                queue_size = await self.distributed_manager.get_queue_size()
                self.stats['queue_size'] = queue_size
                self.stats['uptime'] = time.time() - self.stats['start_time']

                # 每分钟记录一次状态
                if int(time.time()) % 60 == 0:
                    logger.info(f"Master stats: {self.stats}")

                await asyncio.sleep(1)

            except Exception as e:
                logger.error(f"Monitor error: {e}")
                await asyncio.sleep(5)

    async def run(self, seed_urls: List[str] = None):
        """运行主节点"""
        logger.info("Starting master node")
        self.is_running = True

        # 添加种子URL
        if seed_urls:
            await self.add_seed_urls(seed_urls)

        # 启动监控任务
        monitor_task = asyncio.create_task(self.monitor_tasks())

        try:
            # 保持运行
            while self.is_running:
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            logger.info("Shutting down master node")
        finally:
            self.is_running = False
            monitor_task.cancel()
            await asyncio.gather(monitor_task, return_exceptions=True)