import math
import hashlib
from typing import List, Dict, Any
from bitarray import bitarray


class BloomFilter:
    """布隆过滤器 - 用于高效的去重检查"""

    def __init__(self, capacity: int = 1000000, error_rate: float = 0.01,
                 redis_conn=None, redis_key: str = "bloomfilter"):
        """
        初始化布隆过滤器

        Args:
            capacity: 预期容量
            error_rate: 预期错误率
            redis_conn: Redis连接（如果使用Redis后端）
            redis_key: Redis中存储的键名
        """
        self.capacity = capacity
        self.error_rate = error_rate
        self.redis = redis_conn
        self.redis_key = redis_key

        # 计算最优参数
        self.num_bits = self._calculate_bits(capacity, error_rate)
        self.num_hashes = self._calculate_hashes(self.num_bits, capacity)

        if redis_conn:
            # 使用Redis作为后端
            self.backend = 'redis'
        else:
            # 使用内存作为后端
            self.backend = 'memory'
            self.bit_array = bitarray(self.num_bits)
            self.bit_array.setall(0)

    def _calculate_bits(self, n: int, p: float) -> int:
        """计算需要的比特数"""
        return math.ceil(-(n * math.log(p)) / (math.log(2) ** 2))

    def _calculate_hashes(self, m: int, n: int) -> int:
        """计算需要的哈希函数数量"""
        return math.ceil((m / n) * math.log(2))

    def _hash_functions(self, item: str) -> List[int]:
        """生成多个哈希值"""
        hashes = []
        for i in range(self.num_hashes):
            # 使用不同的种子生成多个哈希
            hash_obj = hashlib.md5(f"{item}_{i}".encode())
            hashes.append(int(hash_obj.hexdigest(), 16) % self.num_bits)
        return hashes

    def add(self, item: str):
        """添加元素到布隆过滤器"""
        positions = self._hash_functions(item)

        if self.backend == 'redis':
            pipeline = self.redis.pipeline()
            for pos in positions:
                pipeline.setbit(self.redis_key, pos, 1)
            pipeline.execute()
        else:
            for pos in positions:
                self.bit_array[pos] = 1

    def contains(self, item: str) -> bool:
        """检查元素是否可能在布隆过滤器中"""
        positions = self._hash_functions(item)

        if self.backend == 'redis':
            pipeline = self.redis.pipeline()
            for pos in positions:
                pipeline.getbit(self.redis_key, pos)
            results = pipeline.execute()
            return all(results)
        else:
            return all(self.bit_array[pos] for pos in positions)

    def add_many(self, items: List[str]):
        """批量添加元素"""
        for item in items:
            self.add(item)

    def contains_many(self, items: List[str]) -> List[bool]:
        """批量检查元素"""
        return [self.contains(item) for item in items]

    def clear(self):
        """清空布隆过滤器"""
        if self.backend == 'redis':
            self.redis.delete(self.redis_key)
        else:
            self.bit_array.setall(0)

    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        return {
            'capacity': self.capacity,
            'error_rate': self.error_rate,
            'num_bits': self.num_bits,
            'num_hashes': self.num_hashes,
            'backend': self.backend
        }


class ScalableBloomFilter:
    """可扩展的布隆过滤器 - 自动扩容"""

    def __init__(self, initial_capacity: int = 100000, error_rate: float = 0.01,
                 scale_factor: int = 2, redis_conn=None):
        self.filters = []
        self.initial_capacity = initial_capacity
        self.error_rate = error_rate
        self.scale_factor = scale_factor
        self.redis = redis_conn

        # 创建第一个过滤器
        self._add_filter()

    def _add_filter(self):
        """添加新的布隆过滤器"""
        capacity = self.initial_capacity * (self.scale_factor ** len(self.filters))
        bloom_filter = BloomFilter(capacity, self.error_rate, self.redis)
        self.filters.append(bloom_filter)

    def add(self, item: str):
        """添加元素"""
        # 添加到所有过滤器
        for bloom_filter in self.filters:
            bloom_filter.add(item)

    def contains(self, item: str) -> bool:
        """检查元素是否存在"""
        # 检查所有过滤器
        for bloom_filter in self.filters:
            if bloom_filter.contains(item):
                return True
        return False

    def clear(self):
        """清空所有过滤器"""
        for bloom_filter in self.filters:
            bloom_filter.clear()
        self.filters = []
        self._add_filter()