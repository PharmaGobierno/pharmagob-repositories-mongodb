from typing import Dict, Optional

from .base import BaseMongoDbRepository


class InventoryCountTasksRepository(BaseMongoDbRepository):
    def increment(
        self,
        entity_id: str,
        *,
        quantities: Dict[str, int],
        umu_id: Optional[str] = None,
    ) -> int:
        """Atomically `$inc` the task counters (e.g. `lines.counted_quantity`)."""
        filter = {"_id": entity_id}
        if umu_id:
            filter.update({"umu_id": umu_id})
        result = self._collection.update_one(
            filter,
            {"$inc": quantities},
            upsert=False,
        )
        return result.modified_count
