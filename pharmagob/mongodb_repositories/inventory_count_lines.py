from typing import Any, Iterator, List, Optional, Tuple, Union

from ._claim import ClaimMixin
from ._utils import convert_conditions_to_mongo
from .base import BaseMongoDbRepository


class InventoryCountLinesRepository(ClaimMixin, BaseMongoDbRepository):
    """Lines are soft deleted (`deleted_at`) and purged later in batches (D-09).

    A line is alive while `deleted_at` is null or missing; every read of this
    repository leaves the soft deleted ones out.
    """

    DEFAULT_PURGE_BATCH_SIZE = 1000
    CLAIM_EXTRA_FILTER = {"deleted_at": None}

    def get(
        self,
        document_id,
        *,
        umu_id: Optional[str] = None,
        sort: Optional[List[Tuple[str, int]]] = None,
        projection: Optional[Union[list, dict]] = None,
    ) -> Optional[dict]:
        filter = {"_id": document_id, "deleted_at": None}
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
        parsed_filter["deleted_at"] = None
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

    def soft_delete_by_task(
        self,
        task_id: str,
        *,
        deleted_at: int,
        umu_id: Optional[str] = None,
    ) -> int:
        """Mark the lines of a task as deleted without removing them.

        `is_open` goes to False so the unique partial index (D-02) releases
        the location contents for other tasks.
        """
        filter = {"task.id": task_id, "deleted_at": None}
        if umu_id:
            filter.update({"umu_id": umu_id})
        result = self._collection.update_many(
            filter,
            {"$set": {"deleted_at": deleted_at, "is_open": False}},
            upsert=False,
        )
        return result.modified_count

    def delete_many(
        self,
        and_conditions: Optional[List[Tuple[str, str, Any]]],
        *,
        limit: Optional[int] = None,
    ) -> int:
        """Physically delete one batch of the lines matching the conditions.

        Used by the deferred purge of soft deleted lines, which sends
        `("deleted_at", "not in", [None])`. At most `limit` documents are
        deleted (default `DEFAULT_PURGE_BATCH_SIZE`); the caller repeats until
        it returns 0. Conditions are required so a bare call can never wipe
        the collection.
        """
        if not and_conditions:
            raise ValueError("delete_many requires at least one condition")
        ids = [
            doc["_id"]
            for doc in self._collection.find(
                convert_conditions_to_mongo(and_conditions),
                projection={"_id": 1},
                limit=limit or self.DEFAULT_PURGE_BATCH_SIZE,
            )
        ]
        if not ids:
            return 0
        result = self._collection.delete_many({"_id": {"$in": ids}})
        return result.deleted_count
