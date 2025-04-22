from typing import List, Optional, Tuple

from .base import BaseMongoDbRepository


class PatientsRepository(BaseMongoDbRepository):
    def search_by_curp(
        self,
        curp: str,
        *,
        created_at_gt: int,
        created_at_lt: int,
        page: int = 1,
        limit: int = BaseMongoDbRepository.DEFAULT_QUERY_LIMIT,
        umu_id: Optional[str] = None,
    ) -> Tuple[int, List[dict]]:
        SEARCH_INDEX = "autocomplete_curp_range_created_at"
        search: dict = {
            "index": SEARCH_INDEX,
            "compound": {
                "must": [
                    {"autocomplete": {"query": curp, "path": "curp"}},
                    {
                        "range": {
                            "path": "created_at",
                            "gt": created_at_gt,
                            "lt": created_at_lt,
                        }
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
