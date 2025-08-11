from typing import Iterator, List, Optional, Tuple, Union

from .base import BaseMongoDbRepository


class DispatchRecordDetailRepository(BaseMongoDbRepository):
    def get_by_dispatch_record_id(
        self,
        dispatch_record_id,
        *,
        umu_id: Optional[str] = None,
        sort: Optional[List[Tuple[str, int]]] = None,
        projection: Optional[Union[list, dict]] = None,
        limit: Optional[int] = None
    ) -> Tuple[int, Iterator[dict]]:
        filter = {"dispatch_record.id": dispatch_record_id}
        if umu_id:
            filter.update({"umu_id": umu_id})
        documents_count: int = self._collection.count_documents(filter)
        documents_cursor = self._collection.find(
            filter,
            sort=sort,
            projection=projection,
            limit=(limit or self.DEFAULT_QUERY_LIMIT),
        )
        return documents_count, map(lambda item: item, documents_cursor)
