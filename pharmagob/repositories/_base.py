from typing import List, Optional, Tuple, Union

from infra.mongodb import MongoDbManager


class BaseMongoDbRepository:
    def __init__(self, db_manager: MongoDbManager, collection_name: str):
        self._collection = db_manager.get_collection(collection_name)

    def create(self, data: dict) -> None:
        self._collection.insert_one(data)

    def update(self, document_id, data: dict, *, upsert: bool = False) -> int:
        result = self._collection.update_one(
            {"_id": document_id},
            {"$set": data},
            upsert=upsert,
        )
        return result.modified_count

    def get(
        self,
        document_id,
        *,
        sort: Optional[List[Tuple[str, int]]] = None,
        projection: Optional[Union[list, dict]] = None,
    ) -> Optional[dict]:
        """Retrieve a document based on its unique identifier.

        Parameters:
            document_id: The unique identifier of the document.

        Returns:
            Optional[dict]: An dict representation of the found document
        """
        filter = {"_id": document_id}
        document_data = self._collection.find_one(
            filter, sort=sort, projection=projection
        )
        return document_data
