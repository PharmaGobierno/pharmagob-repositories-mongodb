from typing import Dict

from .base import BaseMongoDbRepository


class InventoryCountTasksRepository(BaseMongoDbRepository):
    def increment(
        self, task_id: str, *, umu_id: str, counters: Dict[str, int]
    ) -> int:
        """Atomically `$inc` the task counters (e.g. `lines.counted_quantity`)."""
        result = self._collection.update_one(
            {"_id": task_id, "umu_id": umu_id},
            {"$inc": counters},
            upsert=False,
        )
        return result.modified_count
