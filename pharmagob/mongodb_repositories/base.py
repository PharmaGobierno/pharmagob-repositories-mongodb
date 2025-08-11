from typing import Any, Iterator, List, Optional, Tuple, Union

from infra.mongodb import MongoDbManager
from pymongo import ASCENDING, DESCENDING

from ._utils import convert_conditions_to_mongo


class BaseMongoDbRepository:
    ASCENDING_ORDER = ASCENDING
    DESCENDING_ORDER = DESCENDING
    DEFAULT_QUERY_LIMIT = 500

    def __init__(
        self, db_manager: MongoDbManager, collection_name: str, *, verbose: bool = False
    ):
        self._collection = db_manager.get_collection(collection_name)
        self.verbose = verbose

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

    def update_many(
        self, and_conditions: Optional[List[Tuple[str, str, Any]]], *, data: dict
    ) -> int:
        parsed_filter: dict = {}
        if and_conditions:
            parsed_filter = convert_conditions_to_mongo(and_conditions)
        update = {"$set": data}
        result = self._collection.update_many(
            parsed_filter,
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
        umu_id: Optional[str] = None,
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
        if umu_id:
            filter.update({"umu_id": umu_id})
        document_data = self._collection.find_one(
            filter, sort=sort, projection=projection
        )
        return document_data

    def get_paginated(
        self,
        page: int = 1,
        limit: int = DEFAULT_QUERY_LIMIT,
        *,
        umu_id: Optional[str] = None,
        and_conditions: Optional[List[Tuple[str, str, Any]]] = None,
        sort: Optional[List[Tuple[str, int]]] = None,
        projection: Optional[List[str]] = None,
    ) -> Tuple[int, Iterator[dict]]:
        parsed_filter: dict = {}
        if and_conditions:
            parsed_filter = convert_conditions_to_mongo(and_conditions)
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
