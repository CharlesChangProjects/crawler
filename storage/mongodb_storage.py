import motor.motor_asyncio
from typing import Dict, Any, List, Optional
from .base_storage import BaseStorage, StorageType
from config.settings import config

import logging
logger = logging.getLogger(__name__)

class MongoDBStorage(BaseStorage):
    """MongoDB存储 - 使用MongoDB存储数据"""

    def __init__(self, connection_string: str = None, database: str = None, **kwargs):
        super().__init__(StorageType.MONGODB)
        self.connection_string = connection_string or config.storage.mongodb_uri
        self.database_name = database or config.storage.mongodb_db
        self.client = None
        self.db = None

    async def connect(self):
        """连接MongoDB"""
        try:
            self.client = motor.motor_asyncio.AsyncIOMotorClient(self.connection_string)
            self.db = self.client[self.database_name]
            # 测试连接
            await self.client.admin.command('ping')
            self.is_connected = True
            logger.info(f"MongoDB connected to: {self.database_name}")
        except Exception as e:
            logger.error(f"MongoDB connection failed: {e}")
            raise

    async def disconnect(self):
        """断开MongoDB连接"""
        if self.client:
            self.client.close()
            self.is_connected = False
            logger.info("MongoDB disconnected")

    def _get_collection(self, collection: str = None):
        """获取集合对象"""
        if not collection:
            collection = "default"
        return self.db[collection]

    async def save(self, data: Dict[str, Any], collection: str = None, **kwargs) -> str:
        """保存单条数据"""
        coll = self._get_collection(collection)
        result = await coll.insert_one(data)
        return str(result.inserted_id)

    async def save_batch(self, data_list: List[Dict[str, Any]], collection: str = None, **kwargs) -> List[str]:
        """批量保存数据"""
        coll = self._get_collection(collection)
        result = await coll.insert_many(data_list)
        return [str(id) for id in result.inserted_ids]

    async def get(self, id: str, collection: str = None, **kwargs) -> Optional[Dict[str, Any]]:
        """根据ID获取数据"""
        coll = self._get_collection(collection)
        from bson import ObjectId
        try:
            document = await coll.find_one({"_id": ObjectId(id)})
            if document:
                document["_id"] = str(document["_id"])
            return document
        except:
            return None

    async def find(self, query: Dict[str, Any] = None, collection: str = None,
                   limit: int = 100, skip: int = 0, **kwargs) -> List[Dict[str, Any]]:
        """查询数据"""
        coll = self._get_collection(collection)
        query = query or {}
        cursor = coll.find(query).skip(skip).limit(limit)
        results = []
        async for document in cursor:
            document["_id"] = str(document["_id"])
            results.append(document)
        return results

    async def update(self, id: str, data: Dict[str, Any], collection: str = None, **kwargs) -> bool:
        """更新数据"""
        coll = self._get_collection(collection)
        from bson import ObjectId
        try:
            result = await coll.update_one({"_id": ObjectId(id)}, {"$set": data})
            return result.modified_count > 0
        except:
            return False

    async def delete(self, id: str, collection: str = None, **kwargs) -> bool:
        """删除数据"""
        coll = self._get_collection(collection)
        from bson import ObjectId
        try:
            result = await coll.delete_one({"_id": ObjectId(id)})
            return result.deleted_count > 0
        except:
            return False

    async def count(self, query: Dict[str, Any] = None, collection: str = None, **kwargs) -> int:
        """统计数据数量"""
        coll = self._get_collection(collection)
        query = query or {}
        return await coll.count_documents(query)

    async def create_index(self, fields: List[str], collection: str = None, **kwargs):
        """创建索引"""
        coll = self._get_collection(collection)
        index_spec = [(field, 1) for field in fields]
        await coll.create_index(index_spec)

    async def aggregate(self, pipeline: List[Dict[str, Any]], collection: str = None, **kwargs) -> List[Dict[str, Any]]:
        """聚合查询"""
        coll = self._get_collection(collection)
        cursor = coll.aggregate(pipeline)
        results = []
        async for document in cursor:
            document["_id"] = str(document["_id"])
            results.append(document)
        return results