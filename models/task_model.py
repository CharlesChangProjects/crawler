from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Dict, Any, Optional
import uuid


class TaskStatus(Enum):
    """任务状态枚举"""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"
    CANCELLED = "cancelled"


class Priority(Enum):
    """任务优先级枚举"""
    HIGH = 10
    MEDIUM = 5
    LOW = 1
    SEED = 15  # 种子URL最高优先级


@dataclass
class Task:
    """爬虫任务数据模型"""

    # 基础信息
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    url: str
    status: TaskStatus = TaskStatus.PENDING
    priority: Priority = Priority.MEDIUM

    # 时间信息
    created_at: datetime = field(default_factory=datetime.now)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    retry_count: int = 0

    # 执行信息
    worker_id: Optional[str] = None
    attempt_count: int = 0
    max_attempts: int = 3

    # 元数据
    metadata: Dict[str, Any] = field(default_factory=dict)
    headers: Dict[str, str] = field(default_factory=dict)
    cookies: Dict[str, str] = field(default_factory=dict)

    # 结果信息
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    error_stack: Optional[str] = None

    # 性能指标
    download_time: Optional[float] = None
    processing_time: Optional[float] = None
    content_size: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            'id': self.id,
            'url': self.url,
            'status': self.status.value,
            'priority': self.priority.value,
            'created_at': self.created_at.isoformat(),
            'started_at': self.started_at.isoformat() if self.started_at else None,
            'completed_at': self.completed_at.isoformat() if self.completed_at else None,
            'retry_count': self.retry_count,
            'worker_id': self.worker_id,
            'attempt_count': self.attempt_count,
            'max_attempts': self.max_attempts,
            'metadata': self.metadata,
            'headers': self.headers,
            'cookies': self.cookies,
            'result': self.result,
            'error': self.error,
            'error_stack': self.error_stack,
            'download_time': self.download_time,
            'processing_time': self.processing_time,
            'content_size': self.content_size
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Task':
        """从字典创建实例"""
        task = cls(
            id=data.get('id', str(uuid.uuid4())),
            url=data['url'],
            status=TaskStatus(data.get('status', 'pending')),
            priority=Priority(data.get('priority', 5)),
            metadata=data.get('metadata', {}),
            headers=data.get('headers', {}),
            cookies=data.get('cookies', {})
        )

        # 时间字段处理
        if 'created_at' in data and data['created_at']:
            task.created_at = datetime.fromisoformat(data['created_at'])
        if 'started_at' in data and data['started_at']:
            task.started_at = datetime.fromisoformat(data['started_at'])
        if 'completed_at' in data and data['completed_at']:
            task.completed_at = datetime.fromisoformat(data['completed_at'])

        # 其他字段
        task.retry_count = data.get('retry_count', 0)
        task.worker_id = data.get('worker_id')
        task.attempt_count = data.get('attempt_count', 0)
        task.max_attempts = data.get('max_attempts', 3)
        task.result = data.get('result')
        task.error = data.get('error')
        task.error_stack = data.get('error_stack')
        task.download_time = data.get('download_time')
        task.processing_time = data.get('processing_time')
        task.content_size = data.get('content_size')

        return task

    def mark_started(self, worker_id: str):
        """标记任务开始"""
        self.status = TaskStatus.PROCESSING
        self.started_at = datetime.now()
        self.worker_id = worker_id
        self.attempt_count += 1

    def mark_completed(self, result: Dict[str, Any]):
        """标记任务完成"""
        self.status = TaskStatus.COMPLETED
        self.completed_at = datetime.now()
        self.result = result
        self.error = None
        self.error_stack = None

    def mark_failed(self, error: str, error_stack: Optional[str] = None):
        """标记任务失败"""
        self.status = TaskStatus.FAILED
        self.completed_at = datetime.now()
        self.error = error
        self.error_stack = error_stack

    def can_retry(self) -> bool:
        """检查是否可以重试"""
        return (self.status == TaskStatus.FAILED and
                self.retry_count < self.max_attempts and
                self.attempt_count < self.max_attempts)

    def prepare_for_retry(self):
        """准备重试"""
        if self.can_retry():
            self.status = TaskStatus.RETRYING
            self.retry_count += 1
            self.started_at = None
            self.worker_id = None
            self.error = None
            self.error_stack = None

    def get_domain(self) -> str:
        """获取URL的域名"""
        from urllib.parse import urlparse
        return urlparse(self.url).netloc

    def get_processing_time(self) -> Optional[float]:
        """获取处理时间（秒）"""
        if self.started_at and self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None