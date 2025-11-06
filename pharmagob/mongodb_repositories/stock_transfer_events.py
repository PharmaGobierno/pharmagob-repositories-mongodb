from typing import Any, Dict, Iterator, List, Optional, Tuple, Union

from .base import BaseMongoDbRepository


class StockTransferEventsRepository(BaseMongoDbRepository):
    def get_by_stock_transfer_id(
        self,
        stock_transfer_id: str,
        *,
        umu_id: Optional[List[str]] = None,
        limit: Optional[int] = None,
        sort: Optional[List[Tuple[str, int]]] = None,
        projection: Optional[Union[list, dict]] = None,
    ) -> Tuple[int, Iterator[dict]]:
        default_sort = sort
        if default_sort is None:
            default_sort = [
                ("transition_timestamp", BaseMongoDbRepository.DESCENDING_ORDER)
            ]
        filter: Dict[str, Any] = {"stock_transfer_id": stock_transfer_id}
        if umu_id:
            filter.update({"umu_id": umu_id})
        documents_count: int = self._collection.count_documents(filter)
        documents_cursor = self._collection.find(
            filter,
            sort=default_sort,
            projection=projection,
            limit=(limit or self.DEFAULT_QUERY_LIMIT),
        )
        return documents_count, map(lambda item: item, documents_cursor)
