"""Backfill `pending_adjustment_quantity` on existing count lines (H-28, REP-08).

Lines written before domain v1.6.6 have no `pending_adjustment_quantity`. For
every live line in ADJUSTMENT or APPLYING the field becomes the signed
difference of the last round (`counts[-1].counted_quantity -
counts[-1].kardex_snapshot_quantity`), which is what the capture / audit
validation now write when the line enters ADJUSTMENT. Lines in any other
state get an explicit `null` only if they carry a stale value. Idempotent.

    python scripts/backfill_pending_adjustment_quantity.py            # whole database
    python scripts/backfill_pending_adjustment_quantity.py --umu 908  # one UMU
    python scripts/backfill_pending_adjustment_quantity.py --dry-run  # count only

Connection: see `_db.py` (refuses a PROD host unless ALLOW_PROD=1).
"""

from __future__ import annotations

import argparse
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from _db import connect, database_name  # noqa: E402

LINES = "inventory-count-lines"
PENDING_STATUSES = ["ADJUSTMENT", "APPLYING"]
FIELD = "pending_adjustment_quantity"


def pending_filter(umu_id: str | None) -> dict:
    query: dict = {"status": {"$in": PENDING_STATUSES}, "deleted_at": None}
    if umu_id:
        query["umu_id"] = umu_id
    return query


def stale_filter(umu_id: str | None) -> dict:
    query: dict = {"status": {"$nin": PENDING_STATUSES}, FIELD: {"$ne": None}}
    if umu_id:
        query["umu_id"] = umu_id
    return query


PENDING_UPDATE = [
    {
        "$set": {
            FIELD: {
                "$subtract": [
                    {"$last": "$counts.counted_quantity"},
                    {"$last": "$counts.kardex_snapshot_quantity"},
                ]
            }
        }
    }
]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--umu", help="restrict to one UMU (default: whole database)")
    parser.add_argument("--db", help="database name (overrides MONGODB_DB / MONGO_DB_NAME)")
    parser.add_argument("--dry-run", action="store_true", help="only report how many lines would change")
    args = parser.parse_args()

    manager = connect(args.db)
    col = manager.get_database()[LINES]
    print(f"database: {database_name(manager)} | umu: {args.umu or '*'}")

    pending = col.count_documents(pending_filter(args.umu))
    stale = col.count_documents(stale_filter(args.umu))
    print(f"lines in {PENDING_STATUSES}: {pending} | lines with a stale value: {stale}")
    if args.dry_run:
        return 0

    set_result = col.update_many(pending_filter(args.umu), PENDING_UPDATE)
    clear_result = col.update_many(stale_filter(args.umu), {"$set": {FIELD: None}})
    print(f"set {FIELD} on {set_result.modified_count} lines, cleared on {clear_result.modified_count}")

    for doc in col.find(pending_filter(args.umu), {"item.id": 1, "status": 1, FIELD: 1, "task.reference_id": 1}):
        print(f"  {doc['task']['reference_id']} {doc['status']} {doc['item']['id']}: {doc[FIELD]:+d}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
