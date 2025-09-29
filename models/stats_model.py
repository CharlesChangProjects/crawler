from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from collections import defaultdict


@dataclass
class DomainStats:
    """域名统计信息"""
    domain: str
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    total_bytes: int = 0
    avg_response_time: float = 0.0
    last_request: Optional[datetime] = None
    error_count: Dict[str, int] = field(default_factory=lambda: defaultdict(int))
    request_timestamps: List[datetime] = field(default_factory=list)

    def update(self, success: bool, bytes_transferred: int, response_time: float, error_type: Optional[str] = None):
        """更新统计信息"""
        current_time = datetime.now()
        self.total_requests += 1
        if success:
            self.successful_requests += 1
        else:
            self.failed_requests += 1
            if error_type:
                self.error_count[error_type] += 1

        self.total_bytes += bytes_transferred
        self.avg_response_time = (
                (self.avg_response_time * (self.total_requests - 1) + response_time) /
                self.total_requests
        )
        self.last_request = current_time
        self.request_timestamps.append(current_time)

        # 清理过期的请求记录（保留最近1小时）
        one_hour_ago = current_time - timedelta(hours=1)
        self.request_timestamps = [ts for ts in self.request_timestamps if ts > one_hour_ago]

    def get_success_rate(self) -> float:
        """获取成功率"""
        if self.total_requests == 0:
            return 0.0
        return self.successful_requests / self.total_requests

    def get_requests_per_minute(self) -> float:
        """获取每分钟请求数"""
        if not self.request_timestamps:
            return 0.0

        current_time = datetime.now()
        one_minute_ago = current_time - timedelta(minutes=1)
        recent_requests = [ts for ts in self.request_timestamps if ts > one_minute_ago]
        return len(recent_requests)

    def get_requests_per_hour(self) -> float:
        """获取每小时请求数"""
        if not self.request_timestamps:
            return 0.0

        current_time = datetime.now()
        one_hour_ago = current_time - timedelta(hours=1)
        recent_requests = [ts for ts in self.request_timestamps if ts > one_hour_ago]
        return len(recent_requests)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'domain': self.domain,
            'total_requests': self.total_requests,
            'successful_requests': self.successful_requests,
            'failed_requests': self.failed_requests,
            'success_rate': self.get_success_rate(),
            'total_bytes': self.total_bytes,
            'avg_response_time': self.avg_response_time,
            'requests_per_minute': self.get_requests_per_minute(),
            'requests_per_hour': self.get_requests_per_hour(),
            'last_request': self.last_request.isoformat() if self.last_request else None,
            'error_count': dict(self.error_count)
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'DomainStats':
        """从字典创建实例"""
        stats = cls(domain=data['domain'])
        stats.total_requests = data.get('total_requests', 0)
        stats.successful_requests = data.get('successful_requests', 0)
        stats.failed_requests = data.get('failed_requests', 0)
        stats.total_bytes = data.get('total_bytes', 0)
        stats.avg_response_time = data.get('avg_response_time', 0.0)

        if data.get('last_request'):
            stats.last_request = datetime.fromisoformat(data['last_request'])

        stats.error_count = defaultdict(int, data.get('error_count', {}))
        return stats


@dataclass
class WorkerStats:
    """工作节点统计信息"""
    worker_id: str
    total_tasks: int = 0
    completed_tasks: int = 0
    failed_tasks: int = 0
    total_processing_time: float = 0.0
    avg_processing_time: float = 0.0
    last_active: Optional[datetime] = None
    current_load: int = 0  # 当前处理的任务数
    task_timestamps: List[datetime] = field(default_factory=list)
    domain_stats: Dict[str, DomainStats] = field(default_factory=dict)

    def update_task(self, task_completed: bool, processing_time: float, domain: Optional[str] = None):
        """更新任务统计信息"""
        current_time = datetime.now()
        self.total_tasks += 1
        if task_completed:
            self.completed_tasks += 1
        else:
            self.failed_tasks += 1

        self.total_processing_time += processing_time
        self.avg_processing_time = (
                (self.avg_processing_time * (self.total_tasks - 1) + processing_time) /
                self.total_tasks
        )
        self.last_active = current_time
        self.task_timestamps.append(current_time)

        # 清理过期的任务记录（保留最近1小时）
        one_hour_ago = current_time - timedelta(hours=1)
        self.task_timestamps = [ts for ts in self.task_timestamps if ts > one_hour_ago]

        # 更新域名统计
        if domain:
            if domain not in self.domain_stats:
                self.domain_stats[domain] = DomainStats(domain)
            self.domain_stats[domain].update(
                success=task_completed,
                bytes_transferred=0,  # 这里需要实际的数据量
                response_time=processing_time,
                error_type="task_failed" if not task_completed else None
            )

    def update_load(self, current_load: int):
        """更新当前负载"""
        self.current_load = current_load
        self.last_active = datetime.now()

    def get_tasks_per_minute(self) -> float:
        """获取每分钟任务数"""
        if not self.task_timestamps:
            return 0.0

        current_time = datetime.now()
        one_minute_ago = current_time - timedelta(minutes=1)
        recent_tasks = [ts for ts in self.task_timestamps if ts > one_minute_ago]
        return len(recent_tasks)

    def get_tasks_per_hour(self) -> float:
        """获取每小时任务数"""
        if not self.task_timestamps:
            return 0.0

        current_time = datetime.now()
        one_hour_ago = current_time - timedelta(hours=1)
        recent_tasks = [ts for ts in self.task_timestamps if ts > one_hour_ago]
        return len(recent_tasks)

    def get_success_rate(self) -> float:
        """获取任务成功率"""
        if self.total_tasks == 0:
            return 0.0
        return self.completed_tasks / self.total_tasks

    def is_active(self, timeout_seconds: int = 300) -> bool:
        """检查工作节点是否活跃"""
        if not self.last_active:
            return False
        return (datetime.now() - self.last_active).total_seconds() < timeout_seconds

    def to_dict(self) -> Dict[str, Any]:
        return {
            'worker_id': self.worker_id,
            'total_tasks': self.total_tasks,
            'completed_tasks': self.completed_tasks,
            'failed_tasks': self.failed_tasks,
            'success_rate': self.get_success_rate(),
            'total_processing_time': self.total_processing_time,
            'avg_processing_time': self.avg_processing_time,
            'tasks_per_minute': self.get_tasks_per_minute(),
            'tasks_per_hour': self.get_tasks_per_hour(),
            'current_load': self.current_load,
            'last_active': self.last_active.isoformat() if self.last_active else None,
            'is_active': self.is_active(),
            'domain_stats': {domain: stats.to_dict() for domain, stats in self.domain_stats.items()}
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'WorkerStats':
        """从字典创建实例"""
        stats = cls(worker_id=data['worker_id'])
        stats.total_tasks = data.get('total_tasks', 0)
        stats.completed_tasks = data.get('completed_tasks', 0)
        stats.failed_tasks = data.get('failed_tasks', 0)
        stats.total_processing_time = data.get('total_processing_time', 0.0)
        stats.avg_processing_time = data.get('avg_processing_time', 0.0)
        stats.current_load = data.get('current_load', 0)

        if data.get('last_active'):
            stats.last_active = datetime.fromisoformat(data['last_active'])

        # 恢复域名统计
        domain_stats_data = data.get('domain_stats', {})
        for domain, domain_data in domain_stats_data.items():
            stats.domain_stats[domain] = DomainStats.from_dict(domain_data)

        return stats


@dataclass
class SystemStats:
    """系统统计信息"""
    start_time: datetime = field(default_factory=datetime.now)
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    total_bytes_transferred: int = 0
    avg_response_time: float = 0.0
    peak_concurrent_workers: int = 0
    current_workers: int = 0
    domain_stats: Dict[str, DomainStats] = field(default_factory=dict)
    worker_stats: Dict[str, WorkerStats] = field(default_factory=dict)
    error_distribution: Dict[str, int] = field(default_factory=lambda: defaultdict(int))
    request_history: List[Dict[str, Any]] = field(default_factory=list)

    def update_request(self, success: bool, bytes_transferred: int, response_time: float,
                       domain: str, error_type: Optional[str] = None):
        """更新请求统计"""
        self.total_requests += 1
        if success:
            self.successful_requests += 1
        else:
            self.failed_requests += 1
            if error_type:
                self.error_distribution[error_type] += 1

        self.total_bytes_transferred += bytes_transferred
        self.avg_response_time = (
                (self.avg_response_time * (self.total_requests - 1) + response_time) /
                self.total_requests
        )

        # 更新域名统计
        if domain not in self.domain_stats:
            self.domain_stats[domain] = DomainStats(domain)
        self.domain_stats[domain].update(success, bytes_transferred, response_time, error_type)

        # 记录请求历史（保留最近1000条）
        self.request_history.append({
            'timestamp': datetime.now(),
            'success': success,
            'bytes': bytes_transferred,
            'response_time': response_time,
            'domain': domain,
            'error_type': error_type
        })
        if len(self.request_history) > 1000:
            self.request_history.pop(0)

    def update_worker_count(self, current_workers: int):
        """更新工作节点数量"""
        self.current_workers = current_workers
        self.peak_concurrent_workers = max(self.peak_concurrent_workers, current_workers)

    def register_worker(self, worker_id: str):
        """注册工作节点"""
        if worker_id not in self.worker_stats:
            self.worker_stats[worker_id] = WorkerStats(worker_id)
        self.update_worker_count(len(self.worker_stats))

    def unregister_worker(self, worker_id: str):
        """注销工作节点"""
        if worker_id in self.worker_stats:
            del self.worker_stats[worker_id]
        self.update_worker_count(len(self.worker_stats))

    def update_worker_task(self, worker_id: str, task_completed: bool,
                           processing_time: float, domain: Optional[str] = None):
        """更新工作节点任务统计"""
        if worker_id in self.worker_stats:
            self.worker_stats[worker_id].update_task(task_completed, processing_time, domain)

    def get_uptime(self) -> timedelta:
        """获取系统运行时间"""
        return datetime.now() - self.start_time

    def get_success_rate(self) -> float:
        """获取总体成功率"""
        if self.total_requests == 0:
            return 0.0
        return self.successful_requests / self.total_requests

    def get_throughput(self, period: str = 'minute') -> float:
        """获取吞吐量（请求/分钟或请求/小时）"""
        now = datetime.now()
        if period == 'minute':
            cutoff = now - timedelta(minutes=1)
        else:  # hour
            cutoff = now - timedelta(hours=1)

        recent_requests = [req for req in self.request_history if req['timestamp'] > cutoff]
        return len(recent_requests)

    def get_top_domains(self, limit: int = 10) -> List[Dict[str, Any]]:
        """获取请求最多的域名"""
        domains = list(self.domain_stats.values())
        domains.sort(key=lambda x: x.total_requests, reverse=True)
        return [domain.to_dict() for domain in domains[:limit]]

    def get_top_workers(self, limit: int = 10) -> List[Dict[str, Any]]:
        """获取处理任务最多的工作节点"""
        workers = list(self.worker_stats.values())
        workers.sort(key=lambda x: x.total_tasks, reverse=True)
        return [worker.to_dict() for worker in workers[:limit]]

    def get_error_summary(self) -> Dict[str, int]:
        """获取错误摘要"""
        return dict(self.error_distribution)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'start_time': self.start_time.isoformat(),
            'uptime': self.get_uptime().total_seconds(),
            'total_requests': self.total_requests,
            'successful_requests': self.successful_requests,
            'failed_requests': self.failed_requests,
            'success_rate': self.get_success_rate(),
            'total_bytes_transferred': self.total_bytes_transferred,
            'avg_response_time': self.avg_response_time,
            'current_workers': self.current_workers,
            'peak_concurrent_workers': self.peak_concurrent_workers,
            'throughput_per_minute': self.get_throughput('minute'),
            'throughput_per_hour': self.get_throughput('hour'),
            'top_domains': self.get_top_domains(5),
            'top_workers': self.get_top_workers(5),
            'error_distribution': self.get_error_summary(),
            'domain_count': len(self.domain_stats),
            'active_workers': len([w for w in self.worker_stats.values() if w.is_active()])
        }

    def reset(self):
        """重置统计信息（保留运行时间）"""
        current_start_time = self.start_time
        self.__init__()
        self.start_time = current_start_time


@dataclass
class PerformanceStats:
    """性能统计信息"""
    timestamp: datetime = field(default_factory=datetime.now)
    cpu_usage: float = 0.0  # CPU使用率百分比
    memory_usage: float = 0.0  # 内存使用率百分比
    network_throughput: float = 0.0  # 网络吞吐量 bytes/sec
    disk_io: float = 0.0  # 磁盘IO bytes/sec
    active_connections: int = 0
    queue_size: int = 0
    processing_latency: float = 0.0  # 平均处理延迟秒数

    def to_dict(self) -> Dict[str, Any]:
        return {
            'timestamp': self.timestamp.isoformat(),
            'cpu_usage': self.cpu_usage,
            'memory_usage': self.memory_usage,
            'network_throughput': self.network_throughput,
            'disk_io': self.disk_io,
            'active_connections': self.active_connections,
            'queue_size': self.queue_size,
            'processing_latency': self.processing_latency
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'PerformanceStats':
        """从字典创建实例"""
        stats = cls()
        stats.cpu_usage = data.get('cpu_usage', 0.0)
        stats.memory_usage = data.get('memory_usage', 0.0)
        stats.network_throughput = data.get('network_throughput', 0.0)
        stats.disk_io = data.get('disk_io', 0.0)
        stats.active_connections = data.get('active_connections', 0)
        stats.queue_size = data.get('queue_size', 0)
        stats.processing_latency = data.get('processing_latency', 0.0)

        if data.get('timestamp'):
            stats.timestamp = datetime.fromisoformat(data['timestamp'])

        return stats


# 全局统计实例
global_stats = SystemStats()


def get_global_stats() -> SystemStats:
    """获取全局统计实例"""
    return global_stats


def update_global_stats(success: bool, bytes_transferred: int, response_time: float,
                        domain: str, error_type: Optional[str] = None):
    """更新全局统计"""
    global_stats.update_request(success, bytes_transferred, response_time, domain, error_type)
