import asyncio
import logging
import time
from typing import Dict, Any
from downloader.async_downloader import AsyncDownloader
from parser.html_parser import HTMLParser
from storage import get_storage
from core.distributed_manager import DistributedManager
from utils.url_manager import URLManager
from config.settings import config

logger = logging.getLogger(__name__)


class CrawlerWorker:
    def __init__(self, worker_id: str = None):
        self.worker_id = worker_id or config.worker_id
        self.downloader = AsyncDownloader()
        self.distributed_manager = DistributedManager()
        self.url_manager = URLManager()
        self.storage = get_storage()
        self.is_running = False
        self.stats = {
            'processed': 0,
            'success': 0,
            'failed': 0,
            'start_time': time.time()
        }

    async def process_task(self, task: Dict[str, Any]):
        """处理单个任务"""
        url = task['url']
        task_id = task.get('id', hash(url))

        logger.info(f"Processing task {task_id}: {url}")

        # 检查是否已访问
        if self.url_manager.is_visited(url):
            logger.debug(f"URL already visited: {url}")
            return

        try:
            # 下载页面
            async with self.downloader:
                result = await self.downloader.download(url)

            if result['status'] != 200:
                logger.warning(f"Failed to download {url}: Status {result['status']}")
                raise Exception(f"HTTP {result['status']}")

            # 解析页面
            encoding = result.get('encoding', 'utf-8')
            parser = HTMLParser(result['content'].decode(encoding), url)

            # 提取数据
            data = {
                'url': url,
                'content': result['content'],
                'status': result['status'],
                'headers': result['headers'],
                'metadata': parser.extract_metadata(),
                'structured_data': parser.extract_structured_data(),
                'text': parser.extract_text(),
                'worker_id': self.worker_id,
                'timestamp': time.time(),
                'task_metadata': task.get('metadata', {})
            }

            # 保存数据
            await self.storage.save(data)

            # 标记为已访问
            self.url_manager.mark_visited(url)

            # 提取新链接
            new_links = parser.extract_links(url)
            for link in new_links:
                if not self.url_manager.is_visited(link):
                    await self.distributed_manager.push_task({
                        'url': link,
                        'priority': 5,
                        'metadata': {'parent_url': url},
                        'timestamp': time.time()
                    })

            self.stats['success'] += 1
            logger.info(f"Successfully processed: {url}")

            # 发送成功结果
            await self.distributed_manager.push_result({
                'task_id': task_id,
                'url': url,
                'success': True,
                'worker_id': self.worker_id,
                'timestamp': time.time()
            })

        except Exception as e:
            self.stats['failed'] += 1
            logger.error(f"Error processing {url}: {str(e)}")

            # 发送失败结果
            await self.distributed_manager.push_result({
                'task_id': task_id,
                'url': url,
                'success': False,
                'error': str(e),
                'worker_id': self.worker_id,
                'timestamp': time.time()
            })

    async def run(self):
        """运行工作节点"""
        logger.info(f"Starting worker {self.worker_id}")
        self.is_running = True

        while self.is_running:
            try:
                # 获取任务
                task = await self.distributed_manager.pop_task()
                if task:
                    await self.process_task(task)
                    self.stats['processed'] += 1
                else:
                    # 没有任务时等待
                    await asyncio.sleep(1)

                # 每处理10个任务记录一次状态
                if self.stats['processed'] % 10 == 0:
                    logger.info(f"Worker {self.worker_id} stats: {self.stats}")

            except Exception as e:
                logger.error(f"Worker error: {str(e)}")
                await asyncio.sleep(5)

        logger.info(f"Worker {self.worker_id} stopped")