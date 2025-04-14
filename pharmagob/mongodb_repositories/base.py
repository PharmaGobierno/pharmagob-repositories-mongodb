import logging
from copy import deepcopy
from typing import Any, Dict, List, Optional, Tuple, Union

from infra.mongodb import MongoDbManager
from pymongo import ASCENDING, DESCENDING
from pymongo.collection import Collection
from pymongo.cursor import Cursor


class Query:
    ASCENDING_ORDER = ASCENDING
    DESCENDING_ORDER = DESCENDING
    _DEFAULT_QUERY_LIMIT = 500

    def __init__(self, collection: Collection, *, verbose: bool = False):
        self._collection = collection
        self._filter: Dict[str, Any] = {}
        self._verbose = verbose

    def filter(self, **kwargs) -> "Query":
        """Add conditions to the current filter."""
        self._filter.update(kwargs)
        return self

    def clear_filter(self) -> None:
        """Clear current filter (useful after get_one/get_by_id)."""
        self._filter = {}

    def _get_filter(self) -> Dict[str, Any]:
        if self._verbose:
            logging.debug(f"[Query] Using filter: {self._filter}")
        return deepcopy(self._filter)

    def count(self) -> int:
        return self._collection.count_documents(self._get_filter())

    def get_all(
        self,
        *,
        sort: Optional[List[Tuple[str, int]]] = None,
        limit: int = _DEFAULT_QUERY_LIMIT,
        projection: Optional[Union[List[str], Dict[str, int]]] = None,
    ) -> Cursor:
        return self._collection.find(
            self._get_filter(), projection=projection, sort=sort, limit=limit
        )

    def get_one_or_none(
        self,
        *,
        sort: Optional[List[Tuple[str, int]]] = None,
        projection: Optional[Union[List[str], Dict[str, int]]] = None,
    ) -> Optional[Dict[str, Any]]:
        return self._collection.find_one(
            self._get_filter(), projection=projection, sort=sort
        )

    def paginate(
        self,
        page: int = 1,
        limit: int = _DEFAULT_QUERY_LIMIT,
        *,
        sort: Optional[List[Tuple[str, int]]] = None,
        projection: Optional[Union[List[str], Dict[str, int]]] = None,
    ) -> Tuple[int, Cursor]:
        if page < 1:
            logging.warning("Page must be >= 1, using 1 as fallback")
            page = 1
        skip = (page - 1) * limit
        total_count = self.count()
        results = self._collection.find(
            self._get_filter(),
            sort=sort,
            skip=skip,
            limit=limit,
            projection=projection,
        )
        return total_count, results


class BaseMongoDbRepository:
    _DEFAULT_QUERY_LIMIT = 500

    def __init__(self, db_manager: MongoDbManager, collection_name: str):
        self._collection = db_manager.get_collection(collection_name)

    @staticmethod
    def convert_conditions_to_mongo(and_conditions: List[Tuple[str, str, Any]]) -> dict:
        mongo_filter = {}
        operator_map = {
            ">=": "$gte",
            "<=": "$lte",
            ">": "$gt",
            "<": "$lt",
            "=": "$eq",
        }

        for field, op, value in and_conditions:
            mongo_op = operator_map.get(op)
            if not mongo_op:
                raise ValueError(f"Operator not supported: {op}")
            if field not in mongo_filter:
                mongo_filter[field] = {}
            mongo_filter[field][mongo_op] = value
        return mongo_filter

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

    def query_paginated(
        self,
        page: int = 1,
        limit: int = 50,
        and_conditions: Optional[List[tuple]] = None,
        sort: Optional[List[Tuple[str, int]]] = None,
        projection: Optional[List[str]] = None,
    ):
        query: Query = self._query(verbose=True)
        if and_conditions:
            mongo_filter = self.convert_conditions_to_mongo(and_conditions)
            query.filter(**mongo_filter)
        return query.paginate(page, limit, sort=sort, projection=projection)

    def _query(self, *, verbose: bool = False) -> Query:
        return Query(self._collection, verbose=verbose)
