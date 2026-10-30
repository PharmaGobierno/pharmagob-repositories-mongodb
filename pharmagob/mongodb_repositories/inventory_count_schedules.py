from ._claim import ClaimMixin
from .base import BaseMongoDbRepository


class InventoryCountSchedulesRepository(ClaimMixin, BaseMongoDbRepository):
    pass
