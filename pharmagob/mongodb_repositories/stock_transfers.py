from typing import Any, Dict, List, Optional, Tuple

from .base import BaseMongoDbRepository


class StockTransfersRepository(BaseMongoDbRepository):
    def search_by_reference_id(
        self,
        search_str: str,
        *,
        page: int = 1,
        limit: int = BaseMongoDbRepository.DEFAULT_QUERY_LIMIT,
        sort: Optional[Dict[str, int]] = None,
        last_event: Optional[str] = None,
        umu_id: Optional[str] = None,
        foreign_umu_id: Optional[str] = None,
        created_at_gt: Optional[int] = None,
        created_at_lt: Optional[int] = None,
    ) -> Tuple[int, List[dict]]:
        SEARCH_INDEX = "autocomplete_reference_id_range_created_at"
        default_sort = sort
        if default_sort is None:
            default_sort = {"created_at": BaseMongoDbRepository.ASCENDING_ORDER}
        search: dict = {
            "index": SEARCH_INDEX,
            "compound": {
                "must": [
                    {"autocomplete": {"query": search_str, "path": "reference_id"}}
                ]
            },
            "sort": default_sort,
        }
        search["compound"]["filter"] = []
        if umu_id:
            search["compound"]["filter"].append(
                {"equals": {"path": "umu_id", "value": umu_id}}
            )
        if last_event:
            search["compound"]["filter"].append(
                {"equals": {"path": "last_event", "value": last_event}}
            )
        if foreign_umu_id:
            search["compound"]["filter"].append(
                {
                    "equals": {
                        "path": "foreign_location_content.umu_id",
                        "value": foreign_umu_id,
                    }
                }
            )
        if created_at_gt is not None or created_at_lt is not None:
            created_range: Dict[str, Any] = {"path": "created_at"}
            if created_at_gt is not None:
                created_range["gt"] = created_at_gt
            if created_at_lt is not None:
                created_range["lt"] = created_at_lt
            search["compound"]["must"].append({"range": created_range})
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
