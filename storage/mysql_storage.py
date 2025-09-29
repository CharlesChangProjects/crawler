import aiomysql
from typing import Dict, Any, List, Optional
from .base_storage import BaseStorage, StorageType
from config.settings import config

import logging
logger = logging.getLogger(__name__)

class MySQLStorage(BaseStorage):
    """MySQL存储 - 使用MySQL存储数据"""

    def __init__(self, connection_string: str = None, **kwargs):
        super().__init__(StorageType.MYSQL)
        self.connection_string = connection_string or config.storage.mysql_uri
        self.pool = None

    async def connect(self):
        """连接MySQL"""
        try:
            self.pool = await aiomysql.create_pool(self.connection_string)
            self.is_connected = True
            logger.info("MySQL connected successfully")
        except Exception as e:
            logger.error(f"MySQL connection failed: {e}")
            raise

    async def disconnect(self):
        """断开MySQL连接"""
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()
            self.is_connected = False
            logger.info("MySQL disconnected")

    async def _execute_query(self, query: str, params: tuple = None):
        """执行SQL查询"""
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(query, params)
                return await cursor.fetchall()

    async def _execute_command(self, query: str, params: tuple = None):
        """执行SQL命令"""
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(query, params)
                await conn.commit()
                return cursor.lastrowid

    async def save(self, data: Dict[str, Any], collection: str = None, **kwargs) -> str:
        """保存单条数据"""
        table = collection or "items"
        columns = list(data.keys())
        values = list(data.values())
        placeholders = ', '.join(['%s'] * len(columns))

        query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
        last_id = await self._execute_command(query, tuple(values))
        return str(last_id)

    async def save_batch(self, data_list: List[Dict[str, Any]], collection: str = None, **kwargs) -> List[str]:
        """批量保存数据"""
        if not data_list:
            return []

        table = collection or "items"
        columns = list(data_list[0].keys())
        values_list = [tuple(item.values()) for item in data_list]
        placeholders = ', '.join(['%s'] * len(columns))

        query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.executemany(query, values_list)
                await conn.commit()
                # 返回生成的ID列表（需要表有自增ID）
                return [str(cursor.lastrowid - len(data_list) + i + 1) for i in range(len(data_list))]

    async def get(self, id: str, collection: str = None, **kwargs) -> Optional[Dict[str, Any]]:
        """根据ID获取数据"""
        table = collection or "items"
        query = f"SELECT * FROM {table} WHERE id = %s"
        results = await self._execute_query(query, (id,))
        return results[0] if results else None

    async def find(self, query: Dict[str, Any] = None, collection: str = None,
                   limit: int = 100, skip: int = 0, **kwargs) -> List[Dict[str, Any]]:
        """查询数据"""
        table = collection or "items"
        where_clause = ""
        params = []

        if query:
            conditions = []
            for key, value in query.items():
                conditions.append(f"{key} = %s")
                params.append(value)
            where_clause = f"WHERE {' AND '.join(conditions)}"

        query_sql = f"SELECT * FROM {table} {where_clause} LIMIT %s OFFSET %s"
        params.extend([limit, skip])

        return await self._execute_query(query_sql, tuple(params))

    async def update(self, id: str, data: Dict[str, Any], collection: str = None, **kwargs) -> bool:
        """更新数据"""
        table = collection or "items"
        set_clause = ', '.join([f"{key} = %s" for key in data.keys()])
        query = f"UPDATE {table} SET {set_clause} WHERE id = %s"
        params = list(data.values()) + [id]

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(query, tuple(params))
                await conn.commit()
                return cursor.rowcount > 0

    async def delete(self, id: str, collection: str = None, **kwargs) -> bool:
        """删除数据"""
        table = collection or "items"
        query = f"DELETE FROM {table} WHERE id = %s"

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(query, (id,))
                await conn.commit()
                return cursor.rowcount > 0

    async def count(self, query: Dict[str, Any] = None, collection: str = None, **kwargs) -> int:
        """统计数据数量"""
        table = collection or "items"
        where_clause = ""
        params = []

        if query:
            conditions = []
            for key, value in query.items():
                conditions.append(f"{key} = %s")
                params.append(value)
            where_clause = f"WHERE {' AND '.join(conditions)}"

        query_sql = f"SELECT COUNT(*) as count FROM {table} {where_clause}"
        results = await self._execute_query(query_sql, tuple(params))
        return results[0]['count'] if results else 0

    async def create_table(self, table_name: str, columns: Dict[str, str]):
        """创建数据表"""
        column_defs = []
        for name, col_type in columns.items():
            column_defs.append(f"{name} {col_type}")

        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(column_defs)})"
        await self._execute_command(query)