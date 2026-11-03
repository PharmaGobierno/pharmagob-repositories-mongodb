from enum import Enum
from typing import Any, Dict, List, Optional

from pymongo import ReturnDocument


class ClaimMixin:
    """Atomic state claim for the repositories whose documents have `status`.

    Mixed into a `BaseMongoDbRepository` subclass, which provides `_collection`.
    """

    CLAIM_EXTRA_FILTER: Dict[str, Any] = {}

    def claim(
        self,
        entity_id: str,
        *,
        expected_statuses: List[str],
        data: dict,
        umu_id: Optional[str] = None,
    ) -> Optional[dict]:
        """`$set` `data` only if the status is one of `expected_statuses`.

        Single `find_one_and_update`, so of N concurrent claims exactly one
        gets the updated document and the rest get None (the document already
        moved on, or it does not exist).
        """
        filter: Dict[str, Any] = {
            "_id": entity_id,
            "status": {
                "$in": [
                    status.value if isinstance(status, Enum) else status
                    for status in expected_statuses
                ]
            },
            **self.CLAIM_EXTRA_FILTER,
        }
        if umu_id:
            filter["umu_id"] = umu_id
        return self._collection.find_one_and_update(  # type: ignore[attr-defined]
            filter,
            {"$set": data},
            upsert=False,
            return_document=ReturnDocument.AFTER,
        )
