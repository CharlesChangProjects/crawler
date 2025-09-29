from abc import ABC, abstractmethod
from enum import Enum
from typing import Dict, Any, List, Optional
import logging

logger = logging.getLogger(__name__)


class StorageType(Enum):
    """存储类型枚举"""
    FILE = "file"
    MONGODB = "mongodb"
    MYSQL = "mysql"
    ELASTICSEARCH = "elasticsearch"


class BaseStorage(ABC):
    """存储基类 - 定义统一的存储接口"""

    def __init__(self, storage_type: StorageType):
        self.storage_type = storage_type
        self.is_connected = False

    @abstractmethod
    async def connect(self):
        """连接存储"""
        pass

    @abstractmethod
    async def disconnect(self):
        """断开存储连接"""
        pass

    @abstractmethod
    async def save(self, data: Dict[str, Any], collection: str = None, **kwargs) -> str:
        """保存单条数据"""
        pass

    @abstractmethod
    async def save_batch(self, data_list: List[Dict[str, Any]], collection: str = None, **kwargs) -> List[str]:
        """批量保存数据"""
        pass

    @abstractmethod
    async def get(self, id: str, collection: str = None, **kwargs) -> Optional[Dict[str, Any]]:
        """根据ID获取数据"""
        pass

    @abstractmethod
    async def find(self, query: Dict[str, Any] = None, collection: str = None,
                   limit: int = 100, skip: int = 0, **kwargs) -> List[Dict[str, Any]]:
        """查询数据"""
        pass

    @abstractmethod
    async def update(self, id: str, data: Dict[str, Any], collection: str = None, **kwargs) -> bool:
        """更新数据"""
        pass

    @abstractmethod
    async def delete(self, id: str, collection: str = None, **kwargs) -> bool:
        """删除数据"""
        pass

    @abstractmethod
    async def count(self, query: Dict[str, Any] = None, collection: str = None, **kwargs) -> int:
        """统计数据数量"""
        pass

    async def __aenter__(self):
        """异步上下文管理器入口"""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """异步上下文管理器退出"""
        await self.disconnect()

    def get_stats(self) -> Dict[str, Any]:
        """获取存储统计信息"""
        return {
            'storage_type': self.storage_type.value,
            'is_connected': self.is_connected
        }

    async def health_check(self) -> bool:
        """健康检查"""
        try:
            # 简单的ping操作来检查连接
            await self.count()
            return True
        except Exception as e:
            logger.error(f"Storage health check failed: {e}")
            return False

    async def create_index(self, fields: List[str], collection: str = None, **kwargs):
        """创建索引（可选实现）"""
        pass

    async def backup(self, backup_path: str, **kwargs):
        """备份数据（可选实现）"""
        pass

    async def restore(self, backup_path: str, **kwargs):
        """恢复数据（可选实现）"""
        pass