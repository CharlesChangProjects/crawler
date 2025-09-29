from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk
from typing import Dict, Any, List, Optional
import logging
from datetime import datetime
from .base_storage import BaseStorage, StorageType
from config.settings import config

logger = logging.getLogger(__name__)


class ElasticsearchStorage(BaseStorage):
    """Elasticsearch存储 - 使用Elasticsearch存储和检索数据"""

    def __init__(self, hosts: List[str] = None, **kwargs):
        super().__init__(StorageType.ELASTICSEARCH)
        self.hosts = hosts or config.storage.elasticsearch_hosts
        self.client = None
        self.default_index = "crawler_data"
        self.settings = {
            "number_of_shards": 1,
            "number_of_replicas": 0,
            "refresh_interval": "1s"
        }
        self.mappings = {
            "properties": {
                "url": {"type": "keyword"},
                "domain": {"type": "keyword"},
                "title": {"type": "text", "analyzer": "standard"},
                "content": {"type": "text", "analyzer": "standard"},
                "status_code": {"type": "integer"},
                "content_type": {"type": "keyword"},
                "crawled_at": {"type": "date"},
                "metadata": {"type": "object", "enabled": True},
                "structured_data": {"type": "object", "enabled": True},
                "text_content": {"type": "text", "analyzer": "standard"},
                "worker_id": {"type": "keyword"},
                "processing_time": {"type": "float"},
                "content_size": {"type": "long"},
                "headers": {"type": "object", "enabled": False},
                "cookies": {"type": "object", "enabled": False}
            }
        }

    async def connect(self):
        """连接Elasticsearch"""
        try:
            self.client = AsyncElasticsearch(
                hosts=self.hosts,
                max_retries=3,
                retry_on_timeout=True
            )

            # 测试连接
            if not await self.client.ping():
                raise ConnectionError("Failed to ping Elasticsearch")

            self.is_connected = True
            logger.info("Elasticsearch connected successfully")

            # 确保默认索引存在
            await self._ensure_index_exists(self.default_index)

        except Exception as e:
            logger.error(f"Elasticsearch connection failed: {e}")
            raise

    async def disconnect(self):
        """断开Elasticsearch连接"""
        if self.client:
            await self.client.close()
            self.is_connected = False
            logger.info("Elasticsearch disconnected")

    async def _ensure_index_exists(self, index_name: str):
        """确保索引存在，如果不存在则创建"""
        if not await self.client.indices.exists(index=index_name):
            await self.client.indices.create(
                index=index_name,
                body={
                    "settings": self.settings,
                    "mappings": self.mappings
                }
            )
            logger.info(f"Created Elasticsearch index: {index_name}")

    async def save(self, data: Dict[str, Any], collection: str = None, **kwargs) -> str:
        """保存单条数据"""
        index = collection or self.default_index
        await self._ensure_index_exists(index)

        # 准备文档数据
        document = data.copy()
        if '_id' in document:
            doc_id = document.pop('_id')
        else:
            doc_id = None

        # 添加时间戳
        if 'crawled_at' not in document:
            document['crawled_at'] = datetime.now().isoformat()

        try:
            response = await self.client.index(
                index=index,
                id=doc_id,
                body=document,
                refresh=kwargs.get('refresh', True)
            )

            doc_id = response['_id']
            logger.debug(f"Document saved to Elasticsearch: {doc_id}")
            return doc_id

        except Exception as e:
            logger.error(f"Failed to save document to Elasticsearch: {e}")
            raise

    async def save_batch(self, data_list: List[Dict[str, Any]], collection: str = None, **kwargs) -> List[str]:
        """批量保存数据"""
        if not data_list:
            return []

        index = collection or self.default_index
        await self._ensure_index_exists(index)

        actions = []
        for data in data_list:
            document = data.copy()
            if '_id' in document:
                doc_id = document.pop('_id')
            else:
                doc_id = None

            if 'crawled_at' not in document:
                document['crawled_at'] = datetime.now().isoformat()

            action = {
                "_index": index,
                "_source": document
            }

            if doc_id:
                action["_id"] = doc_id

            actions.append(action)

        try:
            successes, errors = await async_bulk(
                self.client,
                actions,
                refresh=kwargs.get('refresh', True),
                raise_on_error=False
            )

            if errors:
                logger.warning(f"Elasticsearch bulk operation had {len(errors)} errors")

            # 提取成功的文档ID
            success_ids = []
            for action, result in zip(actions, successes):
                if '_id' in action:
                    success_ids.append(action['_id'])
                else:
                    # 从结果中获取自动生成的ID
                    success_ids.append(result['_id'])

            logger.debug(f"Batch saved {len(success_ids)} documents to Elasticsearch")
            return success_ids

        except Exception as e:
            logger.error(f"Failed to save batch to Elasticsearch: {e}")
            raise

    async def get(self, id: str, collection: str = None, **kwargs) -> Optional[Dict[str, Any]]:
        """根据ID获取数据"""
        index = collection or self.default_index

        try:
            response = await self.client.get(
                index=index,
                id=id,
                **kwargs
            )

            if response['found']:
                document = response['_source']
                document['_id'] = response['_id']
                return document
            else:
                return None

        except Exception as e:
            # 文档不存在时会抛出异常
            if "404" in str(e):
                return None
            logger.error(f"Failed to get document from Elasticsearch: {e}")
            raise

    async def find(self, query: Dict[str, Any] = None, collection: str = None,
                   limit: int = 100, skip: int = 0, **kwargs) -> List[Dict[str, Any]]:
        """查询数据"""
        index = collection or self.default_index

        search_body = {
            "query": query or {"match_all": {}},
            "size": limit,
            "from": skip,
            "sort": [{"crawled_at": {"order": "desc"}}]
        }

        # 添加高亮显示（如果查询需要）
        if kwargs.get('highlight'):
            search_body["highlight"] = {
                "fields": {
                    "title": {},
                    "content": {},
                    "text_content": {}
                }
            }

        try:
            response = await self.client.search(
                index=index,
                body=search_body,
                **kwargs
            )

            results = []
            for hit in response['hits']['hits']:
                document = hit['_source']
                document['_id'] = hit['_id']
                document['_score'] = hit.get('_score', 0)

                # 添加高亮结果
                if 'highlight' in hit:
                    document['highlight'] = hit['highlight']

                results.append(document)

            return results

        except Exception as e:
            logger.error(f"Failed to search documents in Elasticsearch: {e}")
            raise

    async def update(self, id: str, data: Dict[str, Any], collection: str = None, **kwargs) -> bool:
        """更新数据"""
        index = collection or self.default_index

        try:
            response = await self.client.update(
                index=index,
                id=id,
                body={"doc": data},
                refresh=kwargs.get('refresh', True)
            )

            success = response['result'] in ['updated', 'noop']
            if success:
                logger.debug(f"Document updated in Elasticsearch: {id}")
            return success

        except Exception as e:
            if "404" in str(e):
                return False
            logger.error(f"Failed to update document in Elasticsearch: {e}")
            raise

    async def delete(self, id: str, collection: str = None, **kwargs) -> bool:
        """删除数据"""
        index = collection or self.default_index

        try:
            response = await self.client.delete(
                index=index,
                id=id,
                refresh=kwargs.get('refresh', True),
                **kwargs
            )

            success = response['result'] == 'deleted'
            if success:
                logger.debug(f"Document deleted from Elasticsearch: {id}")
            return success

        except Exception as e:
            if "404" in str(e):
                return False
            logger.error(f"Failed to delete document from Elasticsearch: {e}")
            raise

    async def count(self, query: Dict[str, Any] = None, collection: str = None, **kwargs) -> int:
        """统计数据数量"""
        index = collection or self.default_index

        try:
            response = await self.client.count(
                index=index,
                body={"query": query or {"match_all": {}}},
                **kwargs
            )

            return response['count']

        except Exception as e:
            logger.error(f"Failed to count documents in Elasticsearch: {e}")
            raise

    async def search(self, search_body: Dict[str, Any], collection: str = None, **kwargs) -> Dict[str, Any]:
        """高级搜索"""
        index = collection or self.default_index

        try:
            response = await self.client.search(
                index=index,
                body=search_body,
                **kwargs
            )

            return response

        except Exception as e:
            logger.error(f"Failed to perform search in Elasticsearch: {e}")
            raise

    async def create_index(self, index_name: str, mappings: Dict[str, Any] = None,
                           settings: Dict[str, Any] = None, **kwargs):
        """创建索引"""
        try:
            if await self.client.indices.exists(index=index_name):
                logger.warning(f"Index already exists: {index_name}")
                return

            create_body = {
                "settings": settings or self.settings,
                "mappings": mappings or self.mappings
            }

            await self.client.indices.create(
                index=index_name,
                body=create_body,
                **kwargs
            )

            logger.info(f"Created Elasticsearch index: {index_name}")

        except Exception as e:
            logger.error(f"Failed to create index in Elasticsearch: {e}")
            raise

    async def delete_index(self, index_name: str, **kwargs):
        """删除索引"""
        try:
            if await self.client.indices.exists(index=index_name):
                await self.client.indices.delete(index=index_name, **kwargs)
                logger.info(f"Deleted Elasticsearch index: {index_name}")
            else:
                logger.warning(f"Index does not exist: {index_name}")

        except Exception as e:
            logger.error(f"Failed to delete index from Elasticsearch: {e}")
            raise

    async def get_index_stats(self, index_name: str = None, **kwargs) -> Dict[str, Any]:
        """获取索引统计信息"""
        index = index_name or self.default_index

        try:
            response = await self.client.indices.stats(index=index, **kwargs)
            return response['indices'][index] if index in response['indices'] else {}

        except Exception as e:
            logger.error(f"Failed to get index stats from Elasticsearch: {e}")
            raise

    async def health_check(self) -> bool:
        """健康检查"""
        try:
            if not self.client:
                return False

            # 检查集群健康状态
            health = await self.client.cluster.health()
            status = health['status']

            # 绿色或黄色状态都是健康的
            return status in ['green', 'yellow']

        except Exception as e:
            logger.error(f"Elasticsearch health check failed: {e}")
            return False

    async def backup_data(self, index_name: str, backup_path: str, **kwargs):
        """备份数据（需要Elasticsearch快照功能）"""
        # 这里只是示例，实际备份需要配置快照仓库
        logger.warning("Backup functionality requires snapshot repository configuration")

    async def restore_data(self, index_name: str, backup_path: str, **kwargs):
        """恢复数据（需要Elasticsearch快照功能）"""
        # 这里只是示例，实际恢复需要配置快照仓库
        logger.warning("Restore functionality requires snapshot repository configuration")

    def get_stats(self) -> Dict[str, Any]:
        """获取存储统计信息"""
        return {
            'storage_type': self.storage_type.value,
            'is_connected': self.is_connected,
            'hosts': self.hosts,
            'default_index': self.default_index
        }


# 为特定类型的数据创建专门的存储类
class ProductElasticsearchStorage(ElasticsearchStorage):
    """产品数据专用的Elasticsearch存储"""

    def __init__(self, hosts: List[str] = None):
        super().__init__(hosts)
        self.default_index = "products"
        self.mappings = {
            "properties": {
                "sku": {"type": "keyword"},
                "manufacturer": {"type": "keyword"},
                "manufacturer_part_number": {"type": "keyword"},
                "name": {"type": "text", "analyzer": "standard"},
                "description": {"type": "text", "analyzer": "standard"},
                "category": {"type": "keyword"},
                "price": {"type": "float"},
                "currency": {"type": "keyword"},
                "stock_quantity": {"type": "integer"},
                "stock_status": {"type": "keyword"},
                "specifications": {"type": "object", "enabled": True},
                "features": {"type": "text", "analyzer": "standard"},
                "images": {"type": "keyword"},
                "source_url": {"type": "keyword"},
                "crawled_at": {"type": "date"},
                "is_active": {"type": "boolean"}
            }
        }


class PageElasticsearchStorage(ElasticsearchStorage):
    """页面数据专用的Elasticsearch存储"""

    def __init__(self, hosts: List[str] = None):
        super().__init__(hosts)
        self.default_index = "pages"
        self.mappings = {
            "properties": {
                "url": {"type": "keyword"},
                "domain": {"type": "keyword"},
                "title": {"type": "text", "analyzer": "standard"},
                "content": {"type": "text", "analyzer": "standard"},
                "status_code": {"type": "integer"},
                "content_type": {"type": "keyword"},
                "crawled_at": {"type": "date"},
                "metadata": {"type": "object", "enabled": True},
                "text_content": {"type": "text", "analyzer": "standard"},
                "worker_id": {"type": "keyword"},
                "processing_time": {"type": "float"},
                "content_size": {"type": "long"}
            }
        }