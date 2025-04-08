from typing import List, Optional, Tuple, Union

from infra.mongodb import MongoDbManager


class BaseMongoDbRepository:
    _DEFAULT_QUERY_LIMIT = 500

    def __init__(self, db_manager: MongoDbManager, collection_name: str):
        self._collection = db_manager.get_collection(collection_name)

    def create(self, data: dict) -> None:
        self._collection.insert_one(data)

    def update(self, document_id, *, data: dict) -> int:
        update = {"$set": data}
        result = self._collection.update_one(
            {"_id": document_id},
            update=update,
            upsert=False,  # Only update, not insert
        )
        return result.modified_count  # number of documents that were modified

    def set(
        self, document_id, *, data: dict, write_only_if_insert: bool = False
    ) -> int:
        update = {"$set": data}
        if write_only_if_insert:  # Only write if document not exists
            update = {"$setOnInsert": data}
        result = self._collection.update_one(
            {"_id": document_id},
            update=update,
            upsert=True,  # updated or insert
        )
        return result.matched_count  # number of documents that matched the _id

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
