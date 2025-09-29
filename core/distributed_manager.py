import json
from typing import Dict, Any, Optional
from config.redis_config import get_redis_connection
from config.settings import config

class DistributedManager:
    def __init__(self):
        self.redis = get_redis_connection()
        self.task_queue = config.redis.task_queue
        self.result_queue = config.redis.result_queue
        self.stats_key = config.redis.stats_key

    async def push_task(self, task: Dict[str, Any]):
        """推送任务到队列"""
        task_json = json.dumps(task)
        self.redis.lpush(self.task_queue, task_json)

    async def pop_task(self) -> Optional[Dict[str, Any]]:
        """从队列获取任务"""
        task_json = self.redis.rpop(self.task_queue)
        if task_json:
            return json.loads(task_json)
        return None

    async def push_result(self, result: Dict[str, Any]):
        """推送结果到队列"""
        result_json = json.dumps(result)
        self.redis.lpush(self.result_queue, result_json)

    async def pop_result(self) -> Optional[Dict[str, Any]]:
        """从结果队列获取结果"""
        result_json = self.redis.rpop(self.result_queue)
        if result_json:
            return json.loads(result_json)
        return None

    async def get_queue_size(self) -> int:
        """获取队列大小"""
        return self.redis.llen(self.task_queue)

    async def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        stats = self.redis.hgetall(self.stats_key)
        return {k: json.loads(v) for k, v in stats.items()}

    async def update_stats(self, key: str, data: Dict[str, Any]):
        """更新统计信息"""
        self.redis.hset(self.stats_key, key, json.dumps(data))