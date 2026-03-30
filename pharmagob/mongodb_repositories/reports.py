from typing import Optional
from time import time

from .base import BaseMongoDbRepository


class ReportRepository(BaseMongoDbRepository):
    def get_by_id(self, report_id: str) -> Optional[dict]:
        data = self._collection.find_one({"_id": report_id})

        if not data:
            return None

        return data

    def update_status(self, report_id: str, status: str, progress: int) -> bool:
        result = self._collection.update_one(
            {"_id": report_id},
            {
                "$set": {
                    "status": status,
                    "progress": progress,
                    "updated_at": round(time() * 1000),
                }
            },
        )
        return result.modified_count > 0
