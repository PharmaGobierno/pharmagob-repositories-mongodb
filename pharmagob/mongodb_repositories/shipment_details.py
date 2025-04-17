from typing import Iterator, List, Optional, Tuple, Union

from .base import BaseMongoDbRepository


class ShipmentDetailRepository(BaseMongoDbRepository):
    def get_by_shipment_id(
        self,
        shipment_id,
        *,
        sort: Optional[List[Tuple[str, int]]] = None,
        projection: Optional[Union[list, dict]] = None,
        limit: Optional[int] = None
    ) -> Tuple[int, Iterator[dict]]:
        """Retrieve a document based on its unique identifier.

        Parameters:
            shipment_id: The shipment_id foreign key of the documents.

        Returns:
            Tuple[int, Iterator[dict]]: The retrieved resources if found
        """
        filter = {"shipment.id": shipment_id}
        documents_count: int = self._collection.count_documents(filter)
        documents_cursor = self._collection.find(
            filter,
            sort=sort,
            projection=projection,
            limit=(limit or self.DEFAULT_QUERY_LIMIT),
        )
        return documents_count, map(lambda item: item, documents_cursor)
