from typing import Optional
from bson import ObjectId
from time import time
from pharmagob.v1.models.reports import ReportRequestModel
from pharmagob.v1.repository_interfaces.reports import ReportRepositoryInterface
from .base import BaseMongoDbRepository

class ReportRepository(BaseMongoDbRepository, ReportRepositoryInterface):
    def get_by_id(self, report_id: str) -> Optional[ReportRequestModel]:
        if not ObjectId.is_valid(report_id):
            return None
            
        data = self._collection.find_one({"_id": ObjectId(report_id)})
        if not data:
            return None
            
        return ReportRequestModel(**data)

    def create(self, report: ReportRequestModel) -> ReportRequestModel:
        report_data = report.dict()
        
        if isinstance(report_data.get("_id"), str):
            report_data["_id"] = ObjectId(report_data["_id"])
            
        self._collection.insert_one(report_data)
        return report

    def update_status(self, report_id: str, status: str, progress: int) -> bool:
        if not ObjectId.is_valid(report_id):
            return False
            
        result = self._collection.update_one(
            {"_id": ObjectId(report_id)},
            {
                "$set": {
                    "status": status,
                    "progress": progress,
                    "updated_at": round(time() * 1000)
                }
            }
        )
        return result.modified_count > 0