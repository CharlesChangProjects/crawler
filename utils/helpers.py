import asyncio
import time
import random
import string
import hashlib
from typing import Any, Callable, Optional, TypeVar, List
from functools import wraps
from urllib.parse import urlparse

T = TypeVar('T')


def generate_id(length: int = 16, prefix: str = '') -> str:
    """生成随机ID"""
    chars = string.ascii_letters + string.digits
    random_part = ''.join(random.choice(chars) for _ in range(length))
    return f"{prefix}{random_part}" if prefix else random_part


def normalize_url(url: str) -> str:
    """标准化URL（简化版本）"""
    parsed = urlparse(url)
    # 移除片段和查询参数
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}"


def retry_async(max_retries: int = 3, delay: float = 1.0,
                backoff: float = 2.0, exceptions: tuple = (Exception,)):
    """异步重试装饰器"""

    def decorator(func: Callable[..., Any]):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            retries = 0
            current_delay = delay

            while retries <= max_retries:
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    retries += 1
                    if retries > max_retries:
                        raise e

                    # 指数退避
                    await asyncio.sleep(current_delay)
                    current_delay *= backoff

                    # 添加随机抖动
                    current_delay *= random.uniform(0.8, 1.2)

            raise Exception("Max retries exceeded")

        return wrapper

    return decorator


def timeout(seconds: float):
    """超时装饰器"""

    def decorator(func: Callable[..., Any]):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                return await asyncio.wait_for(func(*args, **kwargs), timeout=seconds)
            except asyncio.TimeoutError:
                raise TimeoutError(f"Function {func.__name__} timed out after {seconds} seconds")

        return wrapper

    return decorator


def calculate_hash(data: Any) -> str:
    """计算数据的哈希值"""
    if isinstance(data, str):
        data_bytes = data.encode()
    elif isinstance(data, bytes):
        data_bytes = data
    else:
        data_bytes = str(data).encode()

    return hashlib.md5(data_bytes).hexdigest()


def chunk_list(lst: List[Any], chunk_size: int) -> List[List[Any]]:
    """将列表分块"""
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]


async def async_chunk_processing(items: List[Any], process_func: Callable[[Any], Any],
                                 chunk_size: int = 100, max_concurrent: int = 10):
    """异步分块处理"""
    chunks = chunk_list(items, chunk_size)
    results = []

    semaphore = asyncio.Semaphore(max_concurrent)

    async def process_chunk(chunk):
        async with semaphore:
            return await process_func(chunk)

    tasks = [process_chunk(chunk) for chunk in chunks]
    chunk_results = await asyncio.gather(*tasks, return_exceptions=True)

    for result in chunk_results:
        if not isinstance(result, Exception):
            results.extend(result)

    return results


def format_bytes(size: int) -> str:
    """格式化字节大小"""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024.0:
            return f"{size:.2f} {unit}"
        size /= 1024.0
    return f"{size:.2f} PB"


def format_duration(seconds: float) -> str:
    """格式化时间持续时间"""
    if seconds < 1:
        return f"{seconds * 1000:.0f}ms"
    elif seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        return f"{seconds / 60:.1f}m"
    else:
        return f"{seconds / 3600:.1f}h"


class Timer:
    """计时器上下文管理器"""

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.duration = self.end - self.start

    def get_duration(self) -> float:
        return self.duration


from contextlib import asynccontextmanager

@asynccontextmanager
async def async_timer():
    """异步计时器上下文管理器"""
    start = time.time()
    try:
        yield
    finally:
        elapsed = time.time() - start
        print(f"Operation took {elapsed:.2f} seconds")


class AsyncTimer:
    """异步计时器类

    使用示例:
        async with AsyncTimer() as timer:
            await some_async_operation()
        print(f"Total time: {timer.elapsed:.2f}s")
    """

    def __init__(self):
        self.start = 0
        self.elapsed = 0

    async def __aenter__(self):
        self.start = time.time()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.elapsed = time.time() - self.start
        return False


async def measure_time(async_func, *args, **kwargs):
    """测量异步函数执行时间并返回结果和耗时

    使用示例:
        result, elapsed = await measure_time(some_async_function)
    """
    start = time.time()
    result = await async_func(*args, **kwargs)
    elapsed = time.time() - start
    return result, elapsed