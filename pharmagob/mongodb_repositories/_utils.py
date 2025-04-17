from typing import Any, List, Tuple


def convert_conditions_to_mongo(and_conditions: List[Tuple[str, str, Any]]) -> dict:
    mongo_filter = {}
    operator_map = {
        ">=": "$gte",
        "<=": "$lte",
        ">": "$gt",
        "<": "$lt",
        "=": "$eq",
    }

    for field, op, value in and_conditions:
        mongo_op = operator_map.get(op)
        if not mongo_op:
            raise ValueError(f"Operator not supported: {op}")
        if field not in mongo_filter:
            mongo_filter[field] = {}
        mongo_filter[field][mongo_op] = value
    return mongo_filter
