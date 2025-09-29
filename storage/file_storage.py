import json
import aiofiles
import os
import hashlib
from datetime import datetime
from typing import Dict, Any, List, Optional
from .base_storage import BaseStorage, StorageType
from config.settings import config

import logging
logger = logging.getLogger(__name__)

class FileStorage(BaseStorage):
    """文件存储 - 使用本地文件系统存储数据"""

    def __init__(self, base_path: str = None, **kwargs):
        super().__init__(StorageType.FILE)
        self.base_path = base_path or config.storage.file_path
        self.ensure_directory_exists(self.base_path)

    def ensure_directory_exists(self, path: str):
        """确保目录存在"""
        os.makedirs(path, exist_ok=True)

    async def connect(self):
        """连接文件存储（无操作）"""
        self.is_connected = True
        logger.info(f"File storage connected at: {self.base_path}")

    async def disconnect(self):
        """断开文件存储连接（无操作）"""
        self.is_connected = False
        logger.info("File storage disconnected")

    def _get_file_path(self, id: str, collection: str = None) -> str:
        """获取文件路径"""
        if collection:
            collection_path = os.path.join(self.base_path, collection)
            self.ensure_directory_exists(collection_path)
            return os.path.join(collection_path, f"{id}.json")
        else:
            return os.path.join(self.base_path, f"{id}.json")

    def _generate_id(self, data: Dict[str, Any]) -> str:
        """生成唯一ID"""
        # 使用内容哈希作为ID
        content_str = json.dumps(data, sort_keys=True)
        return hashlib.md5(content_str.encode()).hexdigest()

    async def save(self, data: Dict[str, Any], collection: str = None, **kwargs) -> str:
        """保存单条数据"""
        if 'id' not in data:
            data['id'] = self._generate_id(data)

        # 添加时间戳
        data['_created_at'] = datetime.now().isoformat()
        data['_storage_type'] = 'file'

        file_path = self._get_file_path(data['id'], collection)

        try:
            async with aiofiles.open(file_path, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(data, ensure_ascii=False, indent=2))

            logger.debug(f"Data saved to file: {file_path}")
            return data['id']
        except Exception as e:
            logger.error(f"Failed to save data to file: {e}")
            raise

    async def save_batch(self, data_list: List[Dict[str, Any]], collection: str = None, **kwargs) -> List[str]:
        """批量保存数据"""
        ids = []
        for data in data_list:
            try:
                id = await self.save(data, collection, **kwargs)
                ids.append(id)
            except Exception as e:
                logger.error(f"Failed to save item in batch: {e}")
        return ids

    async def get(self, id: str, collection: str = None, **kwargs) -> Optional[Dict[str, Any]]:
        """根据ID获取数据"""
        file_path = self._get_file_path(id, collection)

        if not os.path.exists(file_path):
            return None

        try:
            async with aiofiles.open(file_path, 'r', encoding='utf-8') as f:
                content = await f.read()
                return json.loads(content)
        except Exception as e:
            logger.error(f"Failed to read data from file: {e}")
            return None

    async def find(self, query: Dict[str, Any] = None, collection: str = None,
                   limit: int = 100, skip: int = 0, **kwargs) -> List[Dict[str, Any]]:
        """查询数据（文件存储的查询功能有限）"""
        results = []
        search_path = self.base_path

        if collection:
            search_path = os.path.join(self.base_path, collection)

        if not os.path.exists(search_path):
            return []

        # 获取所有文件
        files = []
        if collection:
            for file_name in os.listdir(search_path):
                if file_name.endswith('.json'):
                    files.append(os.path.join(search_path, file_name))
        else:
            for file_name in os.listdir(search_path):
                if file_name.endswith('.json'):
                    files.append(os.path.join(search_path, file_name))

        # 简单的顺序查询（性能较差，适用于小规模数据）
        files = files[skip:skip + limit]

        for file_path in files:
            try:
                async with aiofiles.open(file_path, 'r', encoding='utf-8') as f:
                    data = json.loads(await f.read())

                # 简单的查询匹配
                if query:
                    match = True
                    for key, value in query.items():
                        if key not in data or data[key] != value:
                            match = False
                            break
                    if match:
                        results.append(data)
                else:
                    results.append(data)

                if len(results) >= limit:
                    break

            except Exception as e:
                logger.error(f"Error reading file {file_path}: {e}")

        return results

    async def update(self, id: str, data: Dict[str, Any], collection: str = None, **kwargs) -> bool:
        """更新数据"""
        existing = await self.get(id, collection)
        if not existing:
            return False

        # 合并数据
        updated_data = {**existing, **data}
        updated_data['_updated_at'] = datetime.now().isoformat()

        try:
            await self.save(updated_data, collection)
            return True
        except Exception as e:
            logger.error(f"Failed to update data: {e}")
            return False

    async def delete(self, id: str, collection: str = None, **kwargs) -> bool:
        """删除数据"""
        file_path = self._get_file_path(id, collection)

        if os.path.exists(file_path):
            try:
                os.remove(file_path)
                return True
            except Exception as e:
                logger.error(f"Failed to delete file: {e}")
                return False
        return False

    async def count(self, query: Dict[str, Any] = None, collection: str = None, **kwargs) -> int:
        """统计数据数量"""
        search_path = self.base_path

        if collection:
            search_path = os.path.join(self.base_path, collection)

        if not os.path.exists(search_path):
            return 0

        count = 0
        for file_name in os.listdir(search_path):
            if file_name.endswith('.json'):
                count += 1

        return count

    async def backup(self, backup_path: str, **kwargs):
        """备份数据"""
        import shutil
        try:
            if os.path.exists(backup_path):
                shutil.rmtree(backup_path)
            shutil.copytree(self.base_path, backup_path)
            logger.info(f"Data backed up to: {backup_path}")
        except Exception as e:
            logger.error(f"Backup failed: {e}")
            raise