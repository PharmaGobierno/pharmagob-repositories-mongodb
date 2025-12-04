from typing import Iterator, List, Optional, Tuple, Union

from .base import BaseMongoDbRepository


class LocationRepository(BaseMongoDbRepository):
    def get_by_umu_id(
        self,
        umu_id,
        *,
        label_code: Optional[str] = None,
        sort: Optional[List[Tuple[str, int]]] = None,
        projection: Optional[Union[list, dict]] = None,
        limit: Optional[int] = None
    ) -> Tuple[int, Iterator[dict]]:
        """Retrieve a document based on its unique identifier.

        Parameters:
            umu_id: The umu_id foreign key of the documents.

        Returns:
            Tuple[int, Iterator[dict]]: The retrieved resources if found
        """
        filter = {"umu_id": umu_id}
        if label_code is not None:
            filter["label_code"] = label_code
        documents_count: int = self._collection.count_documents(filter)
        documents_cursor = self._collection.find(
            filter,
            sort=sort,
            projection=projection,
            limit=(limit or self.DEFAULT_QUERY_LIMIT),
        )
        return documents_count, map(lambda item: item, documents_cursor)
