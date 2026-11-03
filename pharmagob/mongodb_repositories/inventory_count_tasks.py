from typing import Any, Dict, List, Optional, Tuple

from ._claim import ClaimMixin
from ._search import any_of, autocomplete, equals, in_range, paginated_search
from .base import BaseMongoDbRepository

TASK_NAME_SEARCH_INDEX = "autocomplete_task_name_scheduled_at"


class InventoryCountTasksRepository(ClaimMixin, BaseMongoDbRepository):
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

    def search_by_task_name(
        self,
        search_str: str,
        *,
        page: Optional[int] = None,
        limit: Optional[int] = None,
        sort: Optional[Dict[str, int]] = None,
        umu_id: Optional[str] = None,
        count_type: Optional[str] = None,
        status: Optional[List[str]] = None,
        cyclic_mode: Optional[str] = None,
        scheduled_at_gt: Optional[int] = None,
        scheduled_at_lt: Optional[int] = None,
    ) -> Tuple[int, List[dict]]:
        """Partial match of `task_name`, case and accent insensitive (Atlas)."""
        page = page or 1
        limit = limit or BaseMongoDbRepository.DEFAULT_QUERY_LIMIT
        filters: List[dict] = []
        if umu_id:
            filters.append(equals("umu_id", umu_id))
        if count_type:
            filters.append(equals("count_type", count_type))
        if status:
            filters.append(any_of("status", status))
        if cyclic_mode:
            filters.append(equals("cyclic_mode", cyclic_mode))
        scheduled_at = in_range("scheduled_at", scheduled_at_gt, scheduled_at_lt)
        if scheduled_at:
            filters.append(scheduled_at)
        search: dict = {
            "index": TASK_NAME_SEARCH_INDEX,
            "compound": {
                "must": [autocomplete("task_name", search_str)],
                "filter": filters,
            },
            "sort": sort or {"scheduled_at": BaseMongoDbRepository.DESCENDING_ORDER},
        }
        return paginated_search(self._collection, search, page=page, limit=limit)

    def search_by_reference_id(
        self,
        search_str: str,
        *,
        page: Optional[int] = None,
        limit: Optional[int] = None,
        sort: Optional[Dict[str, int]] = None,
        umu_id: Optional[str] = None,
        count_type: Optional[str] = None,
        status: Optional[List[str]] = None,
        cyclic_mode: Optional[str] = None,
        scheduled_at_gt: Optional[int] = None,
        scheduled_at_lt: Optional[int] = None,
    ) -> Tuple[int, List[dict]]:
        """Exact match of the folio (`reference_id`).

        Exact lookup on the unique `{reference_id}` index, so it does not need
        Atlas Search nor wait for its index to catch up.
        """
        page = page or 1
        limit = limit or BaseMongoDbRepository.DEFAULT_QUERY_LIMIT
        filter: Dict[str, Any] = {"reference_id": search_str}
        if umu_id:
            filter["umu_id"] = umu_id
        if count_type:
            filter["count_type"] = count_type
        if status:
            filter["status"] = {"$in": status}
        if cyclic_mode:
            filter["cyclic_mode"] = cyclic_mode
        scheduled_at: Dict[str, int] = {}
        if scheduled_at_gt is not None:
            scheduled_at["$gt"] = scheduled_at_gt
        if scheduled_at_lt is not None:
            scheduled_at["$lt"] = scheduled_at_lt
        if scheduled_at:
            filter["scheduled_at"] = scheduled_at
        default_sort = sort or {"scheduled_at": BaseMongoDbRepository.DESCENDING_ORDER}
        total_count = self._collection.count_documents(filter)
        results = self._collection.find(
            filter,
            sort=list(default_sort.items()),
            skip=limit * (page - 1),
            limit=limit,
        )
        return total_count, list(results)
