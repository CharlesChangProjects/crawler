from .base_storage import BaseStorage, StorageType
from .file_storage import FileStorage
from .mongodb_storage import MongoDBStorage
from .mysql_storage import MySQLStorage
from .elastic_storage import ElasticsearchStorage

__all__ = [
    'BaseStorage', 'StorageType',
    'FileStorage', 'MongoDBStorage',
    'MySQLStorage', 'ElasticsearchStorage',
    'get_storage'
]


def get_storage(storage_type: str = None, **kwargs):
    """获取存储实例的工厂函数"""
    from config.settings import config

    storage_type = storage_type or config.storage.type

    if storage_type == "file":
        return FileStorage(**kwargs)
    elif storage_type == "mongodb":
        return MongoDBStorage(**kwargs)
    elif storage_type == "mysql":
        return MySQLStorage(**kwargs)
    elif storage_type == "elasticsearch":
        return ElasticsearchStorage(**kwargs)
    else:
        raise ValueError(f"Unsupported storage type: {storage_type}")