import asyncio
import heapq
from typing import Dict, Any, Optional
from core.distributed_manager import DistributedManager


class TaskScheduler:
    def __init__(self):
        self.distributed_manager = DistributedManager()
        self.priority_queue = []
        self.task_cache = {}

    async def schedule_task(self, task: Dict[str, Any]):
        """调度任务"""
        priority = task.get('priority', 5)
        heapq.heappush(self.priority_queue, (-priority, task))

    async def get_next_task(self) -> Optional[Dict[str, Any]]:
        """获取下一个任务"""
        if self.priority_queue:
            _, task = heapq.heappop(self.priority_queue)
            return task

        # 从分布式队列获取任务
        return await self.distributed_manager.pop_task()

    async def run_scheduler(self):
        """运行调度器"""
        while True:
            try:
                task = await self.get_next_task()
                if task:
                    # 这里可以添加任务过滤、去重等逻辑
                    await self.distributed_manager.push_task(task)

                await asyncio.sleep(0.1)
            except Exception as e:
                print(f"Scheduler error: {e}")
                await asyncio.sleep(1)