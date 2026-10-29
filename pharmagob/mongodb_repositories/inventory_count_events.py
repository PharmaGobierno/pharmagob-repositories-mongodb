from typing import Any, Dict, Iterator, List, Optional, Tuple, Union

from .base import BaseMongoDbRepository


class InventoryCountEventsRepository(BaseMongoDbRepository):
    def get_by_reference_id(
        self,
        reference_id: str,
        *,
        umu_id: str,
        created_at_gt: Optional[int] = None,
        created_at_lt: Optional[int] = None,
        limit: Optional[int] = None,
        sort: Optional[List[Tuple[str, int]]] = None,
        projection: Optional[Union[list, dict]] = None,
    ) -> Tuple[int, Iterator[dict]]:
        default_sort = sort
        if default_sort is None:
            default_sort = [("created_at", BaseMongoDbRepository.DESCENDING_ORDER)]
        filter: Dict[str, Any] = {
            "umu_id": umu_id,
            "task.reference_id": reference_id,
        }
        created_at: Dict[str, int] = {}
        if created_at_gt is not None:
            created_at["$gte"] = created_at_gt
        if created_at_lt is not None:
            created_at["$lte"] = created_at_lt
        if created_at:
            filter["created_at"] = created_at
        documents_count: int = self._collection.count_documents(filter)
        documents_cursor = self._collection.find(
            filter,
            sort=default_sort,
            projection=projection,
            limit=(limit or self.DEFAULT_QUERY_LIMIT),
        )
        return documents_count, map(lambda item: item, documents_cursor)
