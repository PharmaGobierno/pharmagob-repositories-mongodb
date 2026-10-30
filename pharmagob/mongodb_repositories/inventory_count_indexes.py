"""Regular indexes of the inventory count collections (epic Conteos, 5.11).

Run per environment (idempotent: creating an index that already exists with
the same definition does nothing):

    MONGODB_URI=... MONGODB_DB=... python -m pharmagob.mongodb_repositories.inventory_count_indexes

The Atlas Search indexes used by the `search_by_*` methods are not created
here; they are versioned apart as JSON definitions.
"""

import os
from typing import Dict, List

from pymongo import ASCENDING, IndexModel, MongoClient

TASKS = "inventory-count-tasks"
SCHEDULES = "inventory-count-schedules"
LINES = "inventory-count-lines"
EVENTS = "inventory-count-events"
LOCATION_CONTENTS = "location-contents"

INDEXES: Dict[str, List[IndexModel]] = {
    TASKS: [
        IndexModel(
            [
                ("umu_id", ASCENDING),
                ("count_type", ASCENDING),
                ("status", ASCENDING),
                ("scheduled_at", ASCENDING),
            ],
            name="umu_count_type_status_scheduled_at",
        ),
        IndexModel(
            [("status", ASCENDING), ("scheduled_at", ASCENDING)],
            name="status_scheduled_at",
        ),
        IndexModel(
            [("reference_id", ASCENDING)], unique=True, name="reference_id_unique"
        ),
    ],
    SCHEDULES: [
        IndexModel(
            [("status", ASCENDING), ("next_run_at", ASCENDING)],
            name="status_next_run_at",
        ),
    ],
    LINES: [
        IndexModel(
            [("umu_id", ASCENDING), ("task.id", ASCENDING), ("status", ASCENDING)],
            name="umu_task_status",
        ),
        # Lock between open tasks of the same count type (D-02).
        IndexModel(
            [
                ("umu_id", ASCENDING),
                ("task.count_type", ASCENDING),
                ("location_content_id", ASCENDING),
            ],
            unique=True,
            partialFilterExpression={"is_open": True},
            name="open_line_lock_by_count_type",
        ),
        IndexModel(
            [
                ("umu_id", ASCENDING),
                ("status", ASCENDING),
                ("resolution.resolved_at", ASCENDING),
            ],
            name="umu_status_resolved_at",
        ),
    ],
    EVENTS: [
        IndexModel(
            [("umu_id", ASCENDING), ("created_at", ASCENDING)],
            name="umu_created_at",
        ),
        IndexModel(
            [("umu_id", ASCENDING), ("event", ASCENDING), ("created_at", ASCENDING)],
            name="umu_event_created_at",
        ),
        IndexModel(
            [("umu_id", ASCENDING), ("task.reference_id", ASCENDING)],
            name="umu_task_reference_id",
        ),
    ],
    # Keyset sweep of `LocationContentRepository.get_batch_after`.
    LOCATION_CONTENTS: [
        IndexModel([("umu_id", ASCENDING), ("_id", ASCENDING)], name="umu_id_sweep"),
    ],
}


def create_inventory_count_indexes(db) -> Dict[str, List[str]]:
    """Create every index and return the names ensured, by collection."""
    return {
        collection: db[collection].create_indexes(indexes)
        for collection, indexes in INDEXES.items()
    }


def main() -> None:
    client = MongoClient(os.environ["MONGODB_URI"])
    created = create_inventory_count_indexes(client[os.environ["MONGODB_DB"]])
    for collection, names in created.items():
        print(f"{collection}: {', '.join(names)}")


if __name__ == "__main__":
    main()
