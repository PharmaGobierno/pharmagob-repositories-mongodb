from typing import Any, Dict, List, Optional, Tuple

from .base import BaseMongoDbRepository


class LocationContentRepository(BaseMongoDbRepository):
    def search_by_item(
        self,
        item_id: str,
        *,
        created_at_gt: int,
        created_at_lt: int,
        page: int = 1,
        limit: int = BaseMongoDbRepository.DEFAULT_QUERY_LIMIT,
        umu_id: Optional[str] = None,
        quantity_gt: Optional[int] = None,
        quantity_lt: Optional[int] = None,
        lot: Optional[str] = None
    ) -> Tuple[int, List[dict]]:
        SEARCH_INDEX = "autocomplete_item_id_range_created_at_range_quantity"
        search: dict = {
            "index": SEARCH_INDEX,
            "compound": {
                "must": [
                    {"autocomplete": {"query": item_id, "path": "item.id"}},
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
