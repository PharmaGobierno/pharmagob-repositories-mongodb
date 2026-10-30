from typing import Any, Dict, List, Optional, Tuple

from .base import BaseMongoDbRepository


class InventoryCountEventsRepository(BaseMongoDbRepository):
    def search_by_reference_id(
        self,
        search_str: str,
        *,
        created_at_gt: int,
        created_at_lt: int,
        page: Optional[int] = None,
        limit: Optional[int] = None,
        sort: Optional[Dict[str, int]] = None,
        umu_id: Optional[str] = None,
        event: Optional[List[str]] = None,
        author_id: Optional[str] = None,
    ) -> Tuple[int, List[dict]]:
        """Events of a task folio (`task.reference_id`, exact match).

        The date range is mandatory and exclusive (`$gt`/`$lt`). It is an
        exact lookup, so it runs on the `{umu_id, task.reference_id}` and
        `{umu_id, event, created_at}` indexes instead of Atlas Search.
        """
        page = page or 1
        limit = limit or BaseMongoDbRepository.DEFAULT_QUERY_LIMIT
        default_sort = sort or {"created_at": BaseMongoDbRepository.DESCENDING_ORDER}
        filter: Dict[str, Any] = {
            "task.reference_id": search_str,
            "created_at": {"$gt": created_at_gt, "$lt": created_at_lt},
        }
        if umu_id:
            filter["umu_id"] = umu_id
        if event:
            filter["event"] = {"$in": event}
        if author_id:
            filter["author.id"] = author_id
        documents_count: int = self._collection.count_documents(filter)
        documents_cursor = self._collection.find(
            filter,
            sort=list(default_sort.items()),
            skip=limit * (page - 1),
            limit=limit,
        )
        return documents_count, list(documents_cursor)
