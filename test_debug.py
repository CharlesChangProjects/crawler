# test_debug.py
import asyncio
import sys
import os

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


async def test_master():
    """测试主节点"""
    from core.master_node import CrawlerMaster
    from utils.logger import setup_logging

    setup_logging("DEBUG")

    master = CrawlerMaster()
    seed_urls = [
        "https://www.digikey.cn",
        "https://www.digikey.cn/products/cn"
    ]

    try:
        await master.run(seed_urls)
    except KeyboardInterrupt:
        print("Master stopped by user")


async def test_worker():
    """测试工作节点"""
    from core.worker_node import CrawlerWorker
    from utils.logger import setup_logging

    setup_logging("DEBUG")

    worker = CrawlerWorker("test-worker-1")
    await worker.run()


async def test_standalone():
    """测试独立模式"""
    from downloader.async_downloader import AsyncDownloader
    from parser.html_parser import HTMLParser
    from storage import get_storage
    from utils.logger import setup_logging

    setup_logging("DEBUG")

    urls = [
        "https://www.digikey.cn",
        "https://www.digikey.cn/products/cn"
    ]

    async with AsyncDownloader() as downloader:
        for url in urls:
            try:
                result = await downloader.download(url)
                parser = HTMLParser(result['content'].decode('utf-8'), url)

                print(f"URL: {url}")
                print(f"Title: {parser.extract_metadata().get('title', 'N/A')}")
                print(f"Status: {result['status']}")
                print("-" * 50)

            except Exception as e:
                print(f"Error with {url}: {e}")


if __name__ == "__main__":
    # 选择要测试的功能
    # asyncio.run(test_master())
    # asyncio.run(test_worker())
    asyncio.run(test_standalone())