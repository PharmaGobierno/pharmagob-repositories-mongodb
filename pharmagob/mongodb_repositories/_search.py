"""Building blocks shared by the Atlas Search `search_by_*` methods."""

from typing import Any, Dict, List, Optional, Tuple


def equals(path: str, value: Any) -> dict:
    return {"equals": {"path": path, "value": value}}


def any_of(path: str, values: List[Any]) -> dict:
    return {"in": {"path": path, "value": values}}


def exists(path: str) -> dict:
    """Path present in the document. Careful with optional fields that the
    models write as `null` (`.dict()` keeps every key): Atlas Search treats a
    key with a null value as existing, so `exists` cannot tell `deleted_at:
    null` from `deleted_at: 1760000000000`. Use `has_number` for those."""
    return {"exists": {"path": path}}


def has_number(path: str) -> dict:
    """True only when the field holds a number (epoch ms, quantities): null is
    not indexed as `number`, so a range from 0 leaves it out. This is how the
    soft deleted lines (`deleted_at` numeric) are excluded; verified against
    Atlas in REP-07 (`exists` returned every line, `range` only the deleted)."""
    return {"range": {"path": path, "gte": 0}}


def autocomplete(path: str, query: str) -> dict:
    return {"autocomplete": {"query": query, "path": path}}


def in_range(
    path: str, gt: Optional[int] = None, lt: Optional[int] = None
) -> Optional[dict]:
    """Exclusive range clause, or None when neither bound is given."""
    if gt is None and lt is None:
        return None
    bounds: Dict[str, Any] = {"path": path}
    if gt is not None:
        bounds["gt"] = gt
    if lt is not None:
        bounds["lt"] = lt
    return {"range": bounds}


def paginated_search(
    collection, search: dict, *, page: int, limit: int
) -> Tuple[int, List[dict]]:
    """Run `$search` and return `(total count, page of results)` in one query."""
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
    data: dict = collection.aggregate(pipeline=pipeline).next()
    return data.get("count", 0), data.get("results", [])
