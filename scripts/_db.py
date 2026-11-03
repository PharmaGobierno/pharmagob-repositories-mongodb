"""Connection helper shared by the scripts of this folder.

Two ways to point at a database, checked in this order:

1. `MONGODB_URI` + `MONGODB_DB` (same variables as
   `pharmagob.mongodb_repositories.inventory_count_indexes`).
2. The `MONGO_DB_PROTOCOL / MONGO_DB_DNS / MONGO_DB_NAME / MONGO_DB_USER /
   MONGO_DB_PASSWORD` variables of `infra-mongodb-python`, the ones every
   service already has in its `.env`.

`--db` on the command line overrides the database name of either.
"""

import os
from typing import Optional

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database


class UriDbManager:
    """Minimal stand-in for `infra.mongodb.MongoDbManager` (same methods the
    repositories call), built from a plain connection string."""

    def __init__(self, uri: str, database: str) -> None:
        self._client = MongoClient(uri)
        self._database = self._client[database]

    def get_client(self) -> MongoClient:
        return self._client

    def get_database(self) -> Database:
        return self._database

    def get_collection(self, collection: str) -> Collection:
        return self._database[collection]

    def close(self) -> None:
        self._client.close()


def connect(database: Optional[str] = None):
    uri = os.getenv("MONGODB_URI")
    if uri:
        name = database or os.getenv("MONGODB_DB")
        if not name:
            raise SystemExit("MONGODB_DB (or --db) is required with MONGODB_URI")
        return UriDbManager(uri, name)
    try:
        from infra.mongodb import MongoDbConnectionConf, MongoDbManager
    except ImportError as error:  # pragma: no cover
        raise SystemExit(
            "Set MONGODB_URI + MONGODB_DB, or install infra-mongodb-python and "
            "set the MONGO_DB_* variables"
        ) from error
    return MongoDbManager(MongoDbConnectionConf(database=database))


def database_name(manager) -> str:
    return manager.get_database().name
