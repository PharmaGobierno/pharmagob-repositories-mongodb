from typing import Any, Dict, List, Optional, Tuple

from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError

from .base import BaseMongoDbRepository

# Written only when the upsert creates the document; `quantity`, `last_author`
# and `updated_at` are written by `$inc`/`$set`, and a path cannot appear in two
# operators of the same update.
_INSERT_ONLY_EXCLUDED = {"_id", "quantity", "last_author", "updated_at"}


class LocationContentRepository(BaseMongoDbRepository):
    def search_by_item(
        self,
        search_str: str,
        *,
        page: int = 1,
        limit: int = BaseMongoDbRepository.DEFAULT_QUERY_LIMIT,
        sort: Optional[Dict[str, int]] = None,
        umu_id: Optional[str] = None,
        expiration_date_gt: Optional[int] = None,
        expiration_date_lt: Optional[int] = None,
        quantity_gt: Optional[int] = None,
        quantity_lt: Optional[int] = None,
        lot: Optional[str] = None,
        location_id: Optional[str] = None,
        location_label_code: Optional[str] = None,
        location_label_code_not_in: Optional[List[str]] = None,
    ) -> Tuple[int, List[dict]]:
        page = page or 1
        limit = limit or BaseMongoDbRepository.DEFAULT_QUERY_LIMIT
        SEARCH_INDEX = "autocomplete_item_id_range_expiration_date_range_quantity"
        default_sort = sort
        if default_sort is None:
            default_sort = {"expiration_date": BaseMongoDbRepository.ASCENDING_ORDER}
        search: dict = {
            "index": SEARCH_INDEX,
            "compound": {
                "must": [{"autocomplete": {"query": search_str, "path": "item.id"}}]
            },
            "sort": default_sort,
        }
        if expiration_date_gt is not None or expiration_date_lt is not None:
            expiration_date_range: Dict[str, Any] = {"path": "expiration_date"}
            if expiration_date_gt is not None:
                expiration_date_range["gt"] = expiration_date_gt
            if expiration_date_lt is not None:
                expiration_date_range["lt"] = expiration_date_lt
            search["compound"]["must"].append({"range": expiration_date_range})
        if quantity_gt is not None or quantity_lt is not None:
            quantity_range: Dict[str, Any] = {"path": "quantity"}
            if quantity_gt is not None:
                quantity_range["gt"] = quantity_gt
            if quantity_lt is not None:
                quantity_range["lt"] = quantity_lt
            search["compound"]["must"].append({"range": quantity_range})
        search["compound"]["filter"] = []
        if umu_id:
            search["compound"]["filter"].append(
                {"equals": {"path": "umu_id", "value": umu_id}}
            )
        if lot:
            search["compound"]["filter"].append(
                {"equals": {"path": "lot", "value": lot}}
            )
        if location_id:
            search["compound"]["filter"].append(
                {"equals": {"path": "location.id", "value": location_id}}
            )
        if location_label_code:
            search["compound"]["filter"].append(
                {
                    "equals": {
                        "path": "location.label_code",
                        "value": location_label_code,
                    }
                }
            )
        if location_label_code_not_in:
            search["compound"].setdefault("mustNot", []).extend(
                [
                    {
                        "in": {
                            "path": "location.label_code",
                            "value": location_label_code_not_in,
                        }
                    }
                ]
            )
        pipeline: List[dict] = [
            {"$search": search},
            {
                "$facet": {
                    "results": [{"$skip": limit * (page - 1)}, {"$limit": limit}],
                    "totalCount": [{"$count": "count"}],
                }
            },
            {"$addFields": {"count": {"$arrayElemAt": ["$totalCount.count", 0]}}},
        ]
        aggregation_cursor = self._collection.aggregate(pipeline=pipeline)
        data: dict = aggregation_cursor.next()
        return data.get("count", 0), data.get("results", [])

    def search_by_item_global(
        self,
        search_str: str,
        *,
        page: int = 1,
        limit: int = BaseMongoDbRepository.DEFAULT_QUERY_LIMIT,
        sort: Optional[Dict[str, int]] = None,
        umu_id_in: Optional[List[str]] = None,
        umu_id_not_in: Optional[List[str]] = None,
        expiration_date_gt: Optional[int] = None,
        expiration_date_lt: Optional[int] = None,
        quantity_gt: Optional[int] = None,
        quantity_lt: Optional[int] = None,
        lot: Optional[str] = None,
        location_id: Optional[str] = None,
        location_label_code: Optional[str] = None,
    ) -> Tuple[int, List[dict]]:
        SEARCH_INDEX = "autocomplete_item_id_range_expiration_date_range_quantity"
        default_sort = sort
        if default_sort is None:
            default_sort = {"expiration_date": BaseMongoDbRepository.ASCENDING_ORDER}
        search: dict = {
            "index": SEARCH_INDEX,
            "compound": {
                "must": [{"autocomplete": {"query": search_str, "path": "item.id"}}]
            },
            "sort": default_sort,
        }
        if expiration_date_gt is not None or expiration_date_lt is not None:
            expiration_date_range: Dict[str, Any] = {"path": "expiration_date"}
            if expiration_date_gt is not None:
                expiration_date_range["gt"] = expiration_date_gt
            if expiration_date_lt is not None:
                expiration_date_range["lt"] = expiration_date_lt
            search["compound"]["must"].append({"range": expiration_date_range})
        if quantity_gt is not None or quantity_lt is not None:
            quantity_range: Dict[str, Any] = {"path": "quantity"}
            if quantity_gt is not None:
                quantity_range["gt"] = quantity_gt
            if quantity_lt is not None:
                quantity_range["lt"] = quantity_lt
            search["compound"]["must"].append({"range": quantity_range})
        search["compound"]["filter"] = []
        if lot:
            search["compound"]["filter"].append(
                {"equals": {"path": "lot", "value": lot}}
            )
        if location_id:
            search["compound"]["filter"].append(
                {"equals": {"path": "location.id", "value": location_id}}
            )
        if location_label_code:
            search["compound"]["filter"].append(
                {
                    "equals": {
                        "path": "location.label_code",
                        "value": location_label_code,
                    }
                }
            )
        if umu_id_in:
            search["compound"]["filter"].append(
                {"in": {"path": "umu_id", "value": umu_id_in}}
            )
        if umu_id_not_in:
            search["compound"].setdefault("mustNot", []).extend(
                [{"in": {"path": "umu_id", "value": umu_id_not_in}}]
            )
        pipeline: List[dict] = [
            {"$search": search},
            {
                "$facet": {
                    "results": [{"$skip": limit * (page - 1)}, {"$limit": limit}],
                    "totalCount": [{"$count": "count"}],
                }
            },
            {"$addFields": {"count": {"$arrayElemAt": ["$totalCount.count", 0]}}},
        ]
        aggregation_cursor = self._collection.aggregate(pipeline=pipeline)
        data: dict = aggregation_cursor.next()
        return data.get("count", 0), data.get("results", [])

    def trigger_report_aggregation(
        self, report_id: str, filters: Dict[str, Any]
    ) -> str:
        temp_collection_name = f"report_{report_id}"

        pipeline = [
            {"$match": filters},
            {
                "$group": {
                    "_id": {"umu_id": "$umu_id", "item_id": "$item.id"},
                    "total_quantity": {"$sum": "$quantity"},
                    "description": {"$first": "$item.short_description"},
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "umu_id": "$_id.umu_id",
                    "item_id": "$_id.item_id",
                    "description": "$description",
                    "total_quantity": "$total_quantity",
                    "createdAt": "$$NOW",
                }
            },
            {"$out": temp_collection_name},
        ]

        self._collection.aggregate(pipeline)

        self._db[temp_collection_name].create_index(
            "createdAt", expireAfterSeconds=86400
        )

        return temp_collection_name

    def find_by_logic_triad(
        self, item_id: str, lot: str, location_id: str
    ) -> Optional[dict]:
        query = {"item.id": item_id, "lot": lot, "location.id": location_id}
        return self._collection.find_one(query)


    def get_by_ids(
        self,
        entity_ids: List[str],
        umu_id: str,
    ) -> List[dict]:

        return list(
            self._collection.find(
                {
                    "_id": {"$in": entity_ids},
                    "umu_id": umu_id,
                }
            )
        )

    def increment_quantity(
        self,
        location_content_id: str,
        delta: int,
        *,
        last_author: dict,
        updated_at: int,
        min_quantity: Optional[int] = None,
        umu_id: Optional[str] = None,
    ) -> Optional[dict]:
        """Atomic `$inc` of `quantity`; returns the document AFTER the change.

        A decrement must carry `min_quantity >= abs(delta)`: it goes into the
        filter (`quantity >= min_quantity`), so stock can never go negative.
        Returns None when nothing matched: the location content does not
        exist, belongs to another UMU, or has less than `min_quantity`.
        """
        if delta == 0:
            raise ValueError("delta cannot be 0")
        if delta < 0 and (min_quantity is None or min_quantity < -delta):
            raise ValueError("a decrement requires min_quantity >= abs(delta)")
        filter: Dict[str, Any] = {"_id": location_content_id}
        if umu_id:
            filter["umu_id"] = umu_id
        if min_quantity is not None:
            filter["quantity"] = {"$gte": min_quantity}
        return self._collection.find_one_and_update(
            filter,
            {
                "$inc": {"quantity": delta},
                "$set": {"last_author": last_author, "updated_at": updated_at},
            },
            upsert=False,
            return_document=ReturnDocument.AFTER,
        )

    def upsert_increment_quantity(
        self,
        location_content_id: str,
        delta: int,
        *,
        insert_data: dict,
        last_author: dict,
        updated_at: int,
    ) -> dict:
        """Create the location content with `quantity = delta`, or add `delta`.

        `insert_data` is the full document; only on insert it provides the
        rest of the fields, so an existing document keeps its `item`,
        `expiration_date` and `created_at`. Returns the document AFTER.
        """
        if delta <= 0:
            raise ValueError("the upsert only increments")
        update = {
            "$inc": {"quantity": delta},
            "$set": {"last_author": last_author, "updated_at": updated_at},
            "$setOnInsert": {
                key: value
                for key, value in insert_data.items()
                if key not in _INSERT_ONLY_EXCLUDED
            },
        }
        try:
            return self._upsert(location_content_id, update)
        except DuplicateKeyError:
            # Two first upserts of the same `_id` raced: the loser finds the
            # document on the retry.
            return self._upsert(location_content_id, update)

    def _upsert(self, location_content_id: str, update: dict) -> dict:
        return self._collection.find_one_and_update(
            {"_id": location_content_id},
            update,
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
