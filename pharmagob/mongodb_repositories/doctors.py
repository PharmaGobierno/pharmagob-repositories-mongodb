from typing import Any, Dict, List, Optional, Tuple

from .base import BaseMongoDbRepository


class DoctorsRepository(BaseMongoDbRepository):
    def search_by_employee_or_licence(
        self,
        employee_number: str,
        *,
        page: int = 1,
        limit: int = BaseMongoDbRepository.DEFAULT_QUERY_LIMIT,
        created_at_gt: Optional[int] = None,
        created_at_lt: Optional[int] = None,
        umu_id: Optional[str] = None,
    ) -> Tuple[int, List[dict]]:
        SEARCH_INDEX = "autocomplete_employee_or_licence_range_created_at"
        search: dict = {
            "index": SEARCH_INDEX,
            "compound": {
                "must": [
                    {
                        "autocomplete": {
                            "query": employee_number,
                            "path": "employee_number",
                        }
                    }
                ]
            },
            "sort": {"created_at": BaseMongoDbRepository.DESCENDING_ORDER},
        }
        if created_at_gt is not None or created_at_lt is not None:
            created_at_range: Dict[str, Any] = {"path": "created_at"}
            if created_at_gt is not None:
                created_at_range["gt"] = created_at_gt
            if created_at_lt is not None:
                created_at_range["lt"] = created_at_lt
            search["compound"]["must"].append({"range": created_at_range})
        search["compound"]["filter"] = []
        if umu_id:
            search["compound"]["filter"].append(
                {"equals": {"path": "umu_id", "value": umu_id}}
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

    def search_by_full_name(
        self,
        full_name: str,
        *,
        page: int = 1,
        limit: int = BaseMongoDbRepository.DEFAULT_QUERY_LIMIT,
        created_at_gt: Optional[int] = None,
        created_at_lt: Optional[int] = None,
        umu_id: Optional[str] = None,
    ) -> Tuple[int, List[dict]]:
        SEARCH_INDEX = "autocomplete_fullname_range_created_at"
        search: dict = {
            "index": SEARCH_INDEX,
            "compound": {
                "must": [
                    {
                        "autocomplete": {
                            "query": full_name,
                            "path": "full_name",
                        }
                    }
                ]
            },
            "sort": {"created_at": BaseMongoDbRepository.DESCENDING_ORDER},
        }
        if created_at_gt is not None or created_at_lt is not None:
            created_at_range: Dict[str, Any] = {"path": "created_at"}
            if created_at_gt is not None:
                created_at_range["gt"] = created_at_gt
            if created_at_lt is not None:
                created_at_range["lt"] = created_at_lt
            search["compound"]["must"].append({"range": created_at_range})
        search["compound"]["filter"] = []
        if umu_id:
            search["compound"]["filter"].append(
                {"equals": {"path": "umu_id", "value": umu_id}}
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
