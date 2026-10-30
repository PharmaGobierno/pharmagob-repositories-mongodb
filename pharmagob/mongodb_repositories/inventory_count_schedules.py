from typing import Dict, List, Optional, Tuple

from ._claim import ClaimMixin
from ._search import autocomplete, equals, paginated_search
from .base import BaseMongoDbRepository

SCHEDULE_NAME_SEARCH_INDEX = "autocomplete_schedule_name_created_at"


class InventoryCountSchedulesRepository(ClaimMixin, BaseMongoDbRepository):
    def search_by_schedule_name(
        self,
        search_str: str,
        *,
        page: Optional[int] = None,
        limit: Optional[int] = None,
        sort: Optional[Dict[str, int]] = None,
        umu_id: Optional[str] = None,
        status: Optional[str] = None,
        frequency: Optional[str] = None,
    ) -> Tuple[int, List[dict]]:
        """Partial match of `schedule_name`, case and accent insensitive (Atlas)."""
        page = page or 1
        limit = limit or BaseMongoDbRepository.DEFAULT_QUERY_LIMIT
        filters: List[dict] = []
        if umu_id:
            filters.append(equals("umu_id", umu_id))
        if status:
            filters.append(equals("status", status))
        if frequency:
            filters.append(equals("frequency", frequency))
        search: dict = {
            "index": SCHEDULE_NAME_SEARCH_INDEX,
            "compound": {
                "must": [autocomplete("schedule_name", search_str)],
                "filter": filters,
            },
            "sort": sort or {"created_at": BaseMongoDbRepository.DESCENDING_ORDER},
        }
        return paginated_search(self._collection, search, page=page, limit=limit)
