from typing import Any, Dict, List, Optional, Tuple

from .base import BaseMongoDbRepository


class ShipmentRepository(BaseMongoDbRepository):
    def search_by_order_number(
        self,
        order_number: str,
        *,
        page: int = 1,
        limit: int = BaseMongoDbRepository.DEFAULT_QUERY_LIMIT,
        umu_id: Optional[str] = None,
        created_at_gt: Optional[int] = None,
        created_at_lt: Optional[int] = None,
        review_status: Optional[List[str]] = None
    ) -> Tuple[int, List[dict]]:
        SEARCH_INDEX = "autocomplete_order_number_range_created_at"
        search: dict = {
            "index": SEARCH_INDEX,
            "compound": {
                "must": [
                    {"autocomplete": {"query": order_number, "path": "order_number"}}
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
        search["compound"]["filter"] = []
        if umu_id:
            search["compound"]["filter"].append(
                {"equals": {"path": "umu_id", "value": umu_id}}
            )
        if review_status:
            search["compound"]["filter"].append(
                {"in": {"path": "review_status", "value": review_status}}
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

    def get_review_status(self, shipment_id: str) -> Optional[str]:
        doc = self.collection.find_one(
            {"_id": shipment_id}, 
            {"review_status": 1}
        )
        return doc.get("review_status") if doc else None