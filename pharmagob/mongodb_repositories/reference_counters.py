from typing import Optional

from infra.mongodb import MongoDbManager
from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError

COLLECTION_NAME = "reference-counters"


class ReferenceCounterRepository:
    """Shared `reference-counters` collection: one atomic sequence per key.

    Not a CRUD repository (it does not extend `BaseMongoDbRepository`): the
    only operation is `next_sequence`. Documents are born on first use.
    """

    def __init__(
        self,
        db_manager: MongoDbManager,
        collection_name: str = COLLECTION_NAME,
        *,
        verbose: bool = False,
    ):
        self._collection = db_manager.get_collection(collection_name)
        self.verbose = verbose

    @staticmethod
    def build_key(entity: str, prefix: str, scope: Optional[str] = None) -> str:
        parts = [entity, prefix] + ([scope] if scope else [])
        return ":".join(parts)

    def next_sequence(
        self,
        entity: str,
        prefix: str,
        *,
        scope: Optional[str] = None,
        updated_at: int,
    ) -> int:
        """Atomically increment and return the next value of `entity:prefix[:scope]`."""
        key = self.build_key(entity, prefix, scope)
        update = {
            "$inc": {"seq": 1},
            "$set": {"updated_at": updated_at},
            "$setOnInsert": {"entity": entity, "prefix": prefix, "scope": scope},
        }
        try:
            document = self._increment(key, update)
        except DuplicateKeyError:
            # Two first uses of the same key at the same time: the loser of the
            # upsert race finds the document on the retry.
            document = self._increment(key, update)
        return document["seq"]

    def _increment(self, key: str, update: dict) -> dict:
        return self._collection.find_one_and_update(
            {"_id": key},
            update,
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
