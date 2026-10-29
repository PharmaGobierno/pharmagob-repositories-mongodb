from time import time
from typing import Any, Iterator, List, Optional, Tuple, Union

from ._utils import convert_conditions_to_mongo
from .base import BaseMongoDbRepository

NOT_DELETED = {"$ne": True}


class InventoryCountLinesRepository(BaseMongoDbRepository):
    """Lines are soft deleted (`is_deleted`) and purged later in batches (D-09)."""

    DEFAULT_PURGE_BATCH_SIZE = 1000

    def get(
        self,
        document_id,
        *,
        umu_id: Optional[str] = None,
        sort: Optional[List[Tuple[str, int]]] = None,
        projection: Optional[Union[list, dict]] = None,
    ) -> Optional[dict]:
        filter = {"_id": document_id, "is_deleted": NOT_DELETED}
        if umu_id:
            filter.update({"umu_id": umu_id})
        return self._collection.find_one(filter, sort=sort, projection=projection)

    def get_paginated(
        self,
        page: int = 1,
        limit: int = BaseMongoDbRepository.DEFAULT_QUERY_LIMIT,
        *,
        umu_id: Optional[str] = None,
        and_conditions: Optional[List[Tuple[str, str, Any]]] = None,
        sort: Optional[List[Tuple[str, int]]] = None,
        projection: Optional[List[str]] = None,
    ) -> Tuple[int, Iterator[dict]]:
        parsed_filter: dict = {}
        if and_conditions:
            parsed_filter = convert_conditions_to_mongo(and_conditions)
        parsed_filter["is_deleted"] = NOT_DELETED
        if umu_id:
            parsed_filter.update({"umu_id": umu_id})
        skip = (page - 1) * limit
        total_count = self._collection.count_documents(parsed_filter)
        results = self._collection.find(
            parsed_filter,
            sort=sort,
            skip=skip,
            limit=limit,
            projection=projection,
        )
        return total_count, map(lambda item: item, results)

    def soft_delete_by_task(self, task_id: str, *, umu_id: str) -> int:
        """Mark the lines of a task as deleted without removing them.

        `is_open` goes to False so the unique partial index (D-02) releases
        the location contents for other tasks.
        """
        result = self._collection.update_many(
            {"umu_id": umu_id, "task.id": task_id, "is_deleted": NOT_DELETED},
            {
                "$set": {
                    "is_deleted": True,
                    "deleted_at": round(time() * 1000),
                    "is_open": False,
                }
            },
            upsert=False,
        )
        return result.modified_count

    def delete_many(
        self,
        task_id: str,
        *,
        umu_id: str,
        limit: int = DEFAULT_PURGE_BATCH_SIZE,
    ) -> int:
        """Physically delete one batch of soft deleted lines of a task.

        Returns the deleted count; the caller repeats until it returns 0.
        """
        ids = [
            doc["_id"]
            for doc in self._collection.find(
                {"umu_id": umu_id, "task.id": task_id, "is_deleted": True},
                projection={"_id": 1},
                limit=limit,
            )
        ]
        if not ids:
            return 0
        result = self._collection.delete_many({"_id": {"$in": ids}})
        return result.deleted_count
