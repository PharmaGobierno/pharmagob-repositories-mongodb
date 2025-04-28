from typing import List, Optional, Tuple

from .base import BaseMongoDbRepository


class DispatchRecordRepository(BaseMongoDbRepository):
    def search_by_reference(
        self,
        reference_id: str,
        *,
        created_at_gt: int,
        created_at_lt: int,
        page: int = 1,
        limit: int = BaseMongoDbRepository.DEFAULT_QUERY_LIMIT,
        umu_id: Optional[str] = None,
        dispatch_gt: Optional[int] = None,
        dispatch_lt: Optional[int] = None,
        service: Optional[str] = None
    ) -> Tuple[int, List[dict]]:
        SEARCH_INDEX = "autocomplete_reference_id_range_created_at_and_dispatch"
        search: dict = {
            "index": SEARCH_INDEX,
            "compound": {
                "must": [
                    {"autocomplete": {"query": reference_id, "path": "reference_id"}},
                    {
                        "range": {
                            "path": "created_at",
                            "gt": created_at_gt,
                            "lt": created_at_lt,
                        },
                        "range": {
                            "path": "dispatch_at",
                            "gt": dispatch_gt,
                            "lt": dispatch_lt,
                        },
                    },
                ]
            },
            "sort": {"created_at": BaseMongoDbRepository.DESCENDING_ORDER},
        }
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
