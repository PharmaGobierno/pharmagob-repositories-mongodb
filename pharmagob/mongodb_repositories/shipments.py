from typing import List, Tuple

from .base import BaseMongoDbRepository


class ShipmentRepository(BaseMongoDbRepository):
    def sarch_by_order_number(
        self,
        order_number: str,
        *,
        review_status: str,
        created_at_gt: int,
        created_at_lt: int,
        page: int = 1,
        limit: int = BaseMongoDbRepository.DEFAULT_QUERY_LIMIT,
    ) -> Tuple[int, dict]:
        SEARCH_INDEX = "order-number-partial-match-created-at-updated-at"
        FIELD = "order_number"
        _sort = {"created_at": BaseMongoDbRepository.DESCENDING_ORDER}
        search: dict = {
            "index": SEARCH_INDEX,
            "compound": {
                "must": [
                    {"autocomplete": {"query": order_number, "path": FIELD}},
                    {
                        "range": {
                            "path": "created_at",
                            "gt": created_at_gt,
                            "lt": created_at_lt,
                        }
                    },
                ],
                "filter": [
                    {"equals": {"path": "review_status", "value": review_status}}
                ],
            },
            "sort": _sort,
        }
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
