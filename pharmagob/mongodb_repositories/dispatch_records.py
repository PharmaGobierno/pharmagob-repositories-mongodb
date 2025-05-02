from typing import Any, Dict, List, Optional, Tuple

from .base import BaseMongoDbRepository


class DispatchRecordRepository(BaseMongoDbRepository):
    def search_by_reference(
        self,
        reference_id: str,
        *,
        page: int = 1,
        limit: int = BaseMongoDbRepository.DEFAULT_QUERY_LIMIT,
        umu_id: Optional[str] = None,
        created_at_gt: Optional[int] = None,
        created_at_lt: Optional[int] = None,
        dispatch_gt: Optional[int] = None,
        dispatch_lt: Optional[int] = None,
        service: Optional[str] = None
    ) -> Tuple[int, List[dict]]:
        SEARCH_INDEX = "autocomplete_reference_id_range_created_at_and_dispatch"
        search: dict = {
            "index": SEARCH_INDEX,
            "compound": {
                "must": [
                    {"autocomplete": {"query": reference_id, "path": "reference_id"}}
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
        if dispatch_gt is not None or dispatch_lt is not None:
            dispatch_range: Dict[str, Any] = {"path": "dispatch"}
            if dispatch_gt is not None:
                dispatch_range["gt"] = dispatch_gt
            if dispatch_lt is not None:
                dispatch_range["lt"] = dispatch_lt
            search["compound"]["must"].append({"range": dispatch_range})
        search["compound"]["filter"] = []
        if umu_id:
            search["compound"]["filter"].append(
                {"equals": {"path": "umu_id", "value": umu_id}}
            )
        if service:
            search["compound"]["filter"].append(
                {"equals": {"path": "service", "value": service}}
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
