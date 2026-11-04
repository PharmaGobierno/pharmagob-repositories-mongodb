"""Create (or inspect) the Atlas Search indexes of `atlas_search_indexes/`.

    python scripts/create_atlas_search_indexes.py              # create missing ones, wait for READY
    python scripts/create_atlas_search_indexes.py --status     # print the status of each index
    python scripts/create_atlas_search_indexes.py --recreate   # drop + create (mapping changed)
    python scripts/create_atlas_search_indexes.py --db other   # another database name

Connection: see `_db.py`. Needs MongoDB 7.0+ on Atlas (`createSearchIndexes`
command); it does not work on mongomock nor on Community.
"""

import argparse
import json
import pathlib
import sys
import time
from typing import Dict, List

from pymongo.errors import OperationFailure
from pymongo.operations import SearchIndexModel

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from _db import connect, database_name  # noqa: E402

INDEX_DIR = pathlib.Path(__file__).resolve().parent.parent / "atlas_search_indexes"
READY = "READY"
NAMESPACE_NOT_FOUND = 26
POLL_SECONDS = 5


def load_definitions() -> List[dict]:
    files = sorted(INDEX_DIR.glob("*.json"))
    if not files:
        raise SystemExit(f"no index definitions in {INDEX_DIR}")
    return [json.loads(path.read_text(encoding="utf-8")) for path in files]


def current_status(collection, name: str) -> str:
    try:
        indexes = list(collection.list_search_indexes())
    except OperationFailure as error:
        if error.code == NAMESPACE_NOT_FOUND:  # the collection does not exist yet
            return "MISSING"
        raise
    for index in indexes:
        if index.get("name") == name:
            return index.get("status", "UNKNOWN")
    return "MISSING"


def ensure_collection(db, name: str) -> None:
    """A search index needs the collection to exist (it is filled when documents arrive)."""
    if name not in db.list_collection_names():
        print(f"creating empty collection {name}")
        db.create_collection(name)


def print_status(db, definitions: List[dict]) -> Dict[str, str]:
    statuses: Dict[str, str] = {}
    for definition in definitions:
        collection = db[definition["collectionName"]]
        status = current_status(collection, definition["name"])
        statuses[definition["name"]] = status
        print(f"{definition['collectionName']}.{definition['name']}: {status}")
    return statuses


def wait_until_ready(db, definitions: List[dict], timeout_seconds: int) -> bool:
    deadline = time.time() + timeout_seconds
    while True:
        statuses = print_status(db, definitions)
        if all(status == READY for status in statuses.values()):
            return True
        if time.time() > deadline:
            print(f"timeout after {timeout_seconds}s; indexes still building", file=sys.stderr)
            return False
        time.sleep(POLL_SECONDS)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--db", help="database name (overrides MONGODB_DB / MONGO_DB_NAME)")
    parser.add_argument("--status", action="store_true", help="only print the status")
    parser.add_argument("--recreate", action="store_true", help="drop the index first if it exists")
    parser.add_argument("--timeout", type=int, default=600, help="seconds to wait for READY")
    args = parser.parse_args()

    manager = connect(args.db)
    db = manager.get_database()
    definitions = load_definitions()
    print(f"database: {database_name(manager)}")

    if args.status:
        return 0 if all(s == READY for s in print_status(db, definitions).values()) else 1

    for definition in definitions:
        collection = db[definition["collectionName"]]
        name = definition["name"]
        status = current_status(collection, name)
        if status != "MISSING" and args.recreate:
            print(f"dropping {definition['collectionName']}.{name} ({status})")
            collection.drop_search_index(name)
            while current_status(collection, name) != "MISSING":
                time.sleep(POLL_SECONDS)
            status = "MISSING"
        if status == "MISSING":
            ensure_collection(db, definition["collectionName"])
            print(f"creating {definition['collectionName']}.{name}")
            collection.create_search_index(
                SearchIndexModel(definition={"mappings": definition["mappings"]}, name=name)
            )
        else:
            print(f"{definition['collectionName']}.{name} already exists ({status}), left as is")

    return 0 if wait_until_ready(db, definitions, args.timeout) else 1


if __name__ == "__main__":
    sys.exit(main())
