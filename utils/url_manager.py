import hashlib
import re
import time
from urllib.parse import urlparse, urljoin, urlunparse, parse_qs, urlencode
from typing import List, Optional, Dict, Any
from config.redis_config import get_redis_connection


class URLManager:
    """URL管理器 - 处理URL规范化、去重和域管理"""

    def __init__(self, redis_conn=None):
        self.redis = redis_conn or get_redis_connection()
        self.visited_urls_key = "crawler:visited_urls"
        self.domain_stats_key = "crawler:domain_stats"

    def normalize_url(self, url: str, keep_fragment: bool = False) -> str:
        """标准化URL"""
        try:
            parsed = urlparse(url)

            # 标准化scheme和netloc
            scheme = parsed.scheme.lower()
            netloc = parsed.netloc.lower()

            # 处理www前缀
            if netloc.startswith('www.'):
                netloc = netloc[4:]

            # 标准化路径
            path = parsed.path
            if not path:
                path = '/'
            else:
                # 移除重复的斜杠
                path = re.sub(r'/+', '/', path)
                # 移除末尾的斜杠（可选）
                if path != '/' and path.endswith('/'):
                    path = path[:-1]

            # 标准化查询参数
            query = parsed.query
            if query:
                params = parse_qs(query, keep_blank_values=True)
                # 对参数进行排序
                sorted_params = sorted([(k, v) for k, v in params.items()])
                query = urlencode(sorted_params, doseq=True)

            # 处理片段
            fragment = parsed.fragment if keep_fragment else ''

            # 重建URL
            normalized = urlunparse((scheme, netloc, path, '', query, fragment))
            return normalized

        except Exception as e:
            raise ValueError(f"Failed to normalize URL {url}: {e}")

    def url_to_hash(self, url: str) -> str:
        """将URL转换为哈希值"""
        normalized = self.normalize_url(url)
        return hashlib.md5(normalized.encode()).hexdigest()

    async def is_visited(self, url: str) -> bool:
        """检查URL是否已访问"""
        url_hash = self.url_to_hash(url)
        return bool(self.redis.sismember(self.visited_urls_key, url_hash))

    async def mark_visited(self, url: str):
        """标记URL为已访问"""
        url_hash = self.url_to_hash(url)
        self.redis.sadd(self.visited_urls_key, url_hash)

    async def mark_many_visited(self, urls: List[str]):
        """批量标记URL为已访问"""
        hashes = [self.url_to_hash(url) for url in urls]
        if hashes:
            self.redis.sadd(self.visited_urls_key, *hashes)

    def get_domain(self, url: str) -> str:
        """获取URL的域名"""
        parsed = urlparse(url)
        return parsed.netloc.lower()

    def is_same_domain(self, url1: str, url2: str) -> bool:
        """检查两个URL是否同一域名"""
        return self.get_domain(url1) == self.get_domain(url2)

    def is_internal_link(self, base_url: str, link: str) -> bool:
        """检查链接是否为内部链接"""
        base_domain = self.get_domain(base_url)
        link_domain = self.get_domain(link)
        return base_domain == link_domain

    def make_absolute_url(self, base_url: str, relative_url: str) -> Optional[str]:
        """将相对URL转换为绝对URL"""
        try:
            return urljoin(base_url, relative_url)
        except:
            return None

    async def get_visited_count(self) -> int:
        """获取已访问URL数量"""
        return self.redis.scard(self.visited_urls_key)

    async def clear_visited_urls(self):
        """清空已访问URL记录"""
        self.redis.delete(self.visited_urls_key)

    async def update_domain_stats(self, domain: str, success: bool, response_time: float):
        """更新域名统计信息"""
        stats_key = f"{self.domain_stats_key}:{domain}"

        # 使用哈希存储统计信息
        pipeline = self.redis.pipeline()
        pipeline.hincrby(stats_key, 'total_requests', 1)
        if success:
            pipeline.hincrby(stats_key, 'successful_requests', 1)
        else:
            pipeline.hincrby(stats_key, 'failed_requests', 1)

        # 更新平均响应时间
        pipeline.hget(stats_key, 'avg_response_time')
        results = pipeline.execute()

        current_avg = float(results[3] or 0) if results[3] else 0
        total = int(results[0] or 1)
        new_avg = (current_avg * (total - 1) + response_time) / total

        self.redis.hset(stats_key, 'avg_response_time', new_avg)
        self.redis.hset(stats_key, 'last_updated', int(time.time()))

    async def get_domain_stats(self, domain: str) -> Dict[str, Any]:
        """获取域名统计信息"""
        stats_key = f"{self.domain_stats_key}:{domain}"
        stats = self.redis.hgetall(stats_key)

        return {
            'total_requests': int(stats.get('total_requests', 0)),
            'successful_requests': int(stats.get('successful_requests', 0)),
            'failed_requests': int(stats.get('failed_requests', 0)),
            'avg_response_time': float(stats.get('avg_response_time', 0)),
            'last_updated': int(stats.get('last_updated', 0))
        }

    def filter_urls(self, urls: List[str], allowed_domains: Optional[List[str]] = None,
                    excluded_patterns: Optional[List[str]] = None) -> List[str]:
        """过滤URL列表"""
        filtered_urls = []

        for url in urls:
            try:
                # 检查URL有效性
                parsed = urlparse(url)
                if not parsed.scheme or not parsed.netloc:
                    continue

                # 检查域名限制
                if allowed_domains:
                    domain = self.get_domain(url)
                    if domain not in allowed_domains:
                        continue

                # 检查排除模式
                if excluded_patterns:
                    if any(re.search(pattern, url) for pattern in excluded_patterns):
                        continue

                filtered_urls.append(url)

            except:
                continue

        return filtered_urls