#!/usr/bin/env python3
"""
分布式爬虫框架主入口文件
支持启动主节点、工作节点、监控节点等多种模式
"""

import asyncio
import argparse
import sys
import signal
from typing import List, Optional

# 添加项目根目录到Python路径
sys.path.insert(0, '.')

from core.master_node import CrawlerMaster
from core.worker_node import CrawlerWorker
from utils.logger import setup_logging, get_logger
from utils.metrics import setup_metrics
from storage import get_storage

logger = get_logger(__name__)


class CrawlerApplication:
    """爬虫应用主类"""

    def __init__(self):
        self.master = None
        self.workers = []
        self.is_running = False
        self.metrics_collector = None

    async def initialize(self):
        """初始化应用"""
        # 设置信号处理
        self._setup_signal_handlers()

        # 初始化指标收集器
        self.metrics_collector = setup_metrics(8000)

        logger.info("Crawler application initialized")

    def _setup_signal_handlers(self):
        """设置信号处理"""
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)

    def _handle_shutdown(self, signum, frame):
        """处理关闭信号"""
        logger.info(f"Received signal {signum}, shutting down...")
        self.is_running = False
        asyncio.create_task(self.shutdown())

    async def run_master(self, seed_urls: Optional[List[str]] = None):
        """运行主节点"""
        logger.info("Starting master node")

        self.master = CrawlerMaster()

        # 默认种子URL（如果未提供）
        default_seeds = seed_urls or [
            "https://www.digikey.cn/products/cn",
            "https://www.digikey.cn",
        ]

        try:
            await self.master.run(default_seeds)
        except Exception as e:
            logger.error(f"Master node failed: {e}")
            raise

    async def run_worker(self, worker_id: Optional[str] = None, count: int = 1):
        """运行工作节点"""
        logger.info(f"Starting {count} worker node(s)")

        for i in range(count):
            worker_id_str = worker_id or f"worker-{i + 1}"
            worker = CrawlerWorker(worker_id_str)
            self.workers.append(worker)

            # 启动工作节点（但不等待完成）
            asyncio.create_task(self._run_worker_task(worker))

        # 等待所有工作节点完成
        await asyncio.gather(*[worker.run() for worker in self.workers])

    async def _run_worker_task(self, worker: CrawlerWorker):
        """运行工作节点任务"""
        try:
            await worker.run()
        except Exception as e:
            logger.error(f"Worker {worker.worker_id} failed: {e}")

    async def run_monitor(self):
        """运行监控节点"""
        logger.info("Starting monitor node")

        # 这里可以启动Prometheus、Grafana等监控服务
        # 目前metrics已经在initialize中启动

        try:
            # 监控节点主要工作是收集和展示指标
            while self.is_running:
                # 收集系统指标
                await self._collect_system_metrics()
                await asyncio.sleep(30)

        except Exception as e:
            logger.error(f"Monitor node failed: {e}")
            raise

    async def _collect_system_metrics(self):
        """收集系统指标"""
        # 这里可以添加更多的系统指标收集逻辑
        try:
            # 示例：获取存储统计
            storage = get_storage()
            # 可以添加更多的监控指标收集

        except Exception as e:
            logger.debug(f"Failed to collect system metrics: {e}")

    async def run_standalone(self, urls: List[str]):
        """运行独立模式（调试用）"""
        logger.info("Starting standalone mode")

        from downloader.async_downloader import AsyncDownloader
        from parser.html_parser import HTMLParser

        try:
            async with AsyncDownloader() as downloader:
                for url in urls:
                    try:
                        logger.info(f"Downloading: {url}")
                        result = await downloader.download(url)

                        # 解析内容
                        parser = HTMLParser(result['content'].decode(result.get('encoding', 'utf-8')), url)
                        metadata = parser.extract_metadata()

                        logger.info(f"Title: {metadata.get('title', 'N/A')}")
                        logger.info(f"Status: {result['status']}")
                        logger.info(f"Content size: {len(result['content'])} bytes")

                        # 保存到存储
                        storage = get_storage()
                        data = {
                            'url': url,
                            'content': result['content'],
                            'status': result['status'],
                            'metadata': metadata,
                            'text': parser.extract_text()
                        }
                        await storage.save(data)

                    except Exception as e:
                        logger.error(f"Failed to process {url}: {e}")

        except Exception as e:
            logger.error(f"Standalone mode failed: {e}")
            raise

    async def run_benchmark(self, url: str, requests: int = 100, concurrency: int = 10):
        """运行性能测试"""
        logger.info(f"Starting benchmark: {requests} requests with {concurrency} concurrency")

        from downloader.async_downloader import AsyncDownloader
        import time

        try:
            start_time = time.time()
            success_count = 0
            failure_count = 0

            async with AsyncDownloader() as downloader:
                # 创建任务列表
                tasks = []
                for i in range(requests):
                    task = asyncio.create_task(self._benchmark_task(downloader, f"{url}?test={i}"))
                    tasks.append(task)

                # 限制并发数
                semaphore = asyncio.Semaphore(concurrency)

                async def limited_task(task):
                    async with semaphore:
                        return await task

                # 执行基准测试
                results = await asyncio.gather(*[limited_task(task) for task in tasks], return_exceptions=True)

                # 统计结果
                for result in results:
                    if isinstance(result, Exception):
                        failure_count += 1
                    else:
                        success_count += 1

            end_time = time.time()
            total_time = end_time - start_time
            rps = requests / total_time if total_time > 0 else 0

            logger.info(f"Benchmark completed:")
            logger.info(f"  Total requests: {requests}")
            logger.info(f"  Successful: {success_count}")
            logger.info(f"  Failed: {failure_count}")
            logger.info(f"  Total time: {total_time:.2f}s")
            logger.info(f"  Requests per second: {rps:.2f}")
            logger.info(f"  Success rate: {(success_count / requests * 100):.1f}%")

        except Exception as e:
            logger.error(f"Benchmark failed: {e}")
            raise

    async def _benchmark_task(self, downloader, url: str):
        """基准测试任务"""
        try:
            result = await downloader.download(url)
            return result
        except Exception as e:
            raise e

    async def show_stats(self):
        """显示统计信息"""
        logger.info("Showing system statistics")

        try:
            # 获取存储统计
            storage = get_storage()
            stats = storage.get_stats()

            logger.info("Storage Statistics:")
            logger.info(f"  Type: {stats.get('storage_type', 'N/A')}")
            logger.info(f"  Connected: {stats.get('is_connected', False)}")

            # 这里可以添加更多的统计信息展示

        except Exception as e:
            logger.error(f"Failed to show statistics: {e}")

    async def shutdown(self):
        """关闭应用"""
        logger.info("Shutting down crawler application")

        # 关闭主节点
        if self.master:
            # 这里需要添加主节点的关闭逻辑
            pass

        # 关闭工作节点
        for worker in self.workers:
            # 这里需要添加工作节点的关闭逻辑
            worker.is_running = False

        # 关闭指标收集器
        if self.metrics_collector:
            # 这里可以添加指标收集器的关闭逻辑
            pass

        logger.info("Crawler application shutdown complete")


def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description="分布式爬虫框架")

    # 运行模式
    parser.add_argument(
        "mode",
        choices=["master", "worker", "monitor", "standalone", "benchmark", "stats"],
        help="运行模式: master(主节点), worker(工作节点), monitor(监控节点), standalone(独立模式), benchmark(性能测试), stats(统计信息)"
    )

    # 通用参数
    parser.add_argument(
        "--worker-id",
        help="工作节点ID（worker模式使用）"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="工作节点数量（worker模式使用）"
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="日志级别"
    )
    parser.add_argument(
        "--log-file",
        help="日志文件路径"
    )

    # standalone模式参数
    parser.add_argument(
        "--urls",
        nargs="+",
        help="要爬取的URL列表（standalone模式使用）"
    )

    # benchmark模式参数
    parser.add_argument(
        "--benchmark-url",
        help="基准测试URL（benchmark模式使用）"
    )
    parser.add_argument(
        "--requests",
        type=int,
        default=100,
        help="请求数量（benchmark模式使用）"
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=10,
        help="并发数（benchmark模式使用）"
    )

    # 种子URL参数
    parser.add_argument(
        "--seed-urls",
        nargs="+",
        help="种子URL列表（master模式使用）"
    )

    return parser.parse_args()


async def main():
    """主函数"""
    # 解析命令行参数
    args = parse_arguments()

    # 设置日志
    setup_logging(log_level=args.log_level, log_file=args.log_file)

    # 创建应用实例
    app = CrawlerApplication()
    await app.initialize()

    try:
        # 根据模式运行不同的逻辑
        if args.mode == "master":
            await app.run_master(args.seed_urls)

        elif args.mode == "worker":
            await app.run_worker(args.worker_id, args.workers)

        elif args.mode == "monitor":
            await app.run_monitor()

        elif args.mode == "standalone":
            if not args.urls:
                logger.error("Standalone mode requires --urls argument")
                return
            await app.run_standalone(args.urls)

        elif args.mode == "benchmark":
            if not args.benchmark_url:
                logger.error("Benchmark mode requires --benchmark-url argument")
                return
            await app.run_benchmark(args.benchmark_url, args.requests, args.concurrency)

        elif args.mode == "stats":
            await app.show_stats()

        else:
            logger.error(f"Unknown mode: {args.mode}")
            return

    except KeyboardInterrupt:
        logger.info("Application interrupted by user")
    except Exception as e:
        logger.error(f"Application failed: {e}")
        raise
    finally:
        await app.shutdown()


if __name__ == "__main__":
    # 启动事件循环
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Application shutdown complete")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)