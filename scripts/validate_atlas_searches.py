"""Run the 12 Atlas Search checks of the REP-07 guide (section 2.3) against a
database that has the three indexes in READY and the seed of
`seed_inventory_count_dev.py` loaded.

    python scripts/validate_atlas_searches.py                 # default UMU of the seed
    python scripts/validate_atlas_searches.py --umu MCIMB003562
    python scripts/validate_atlas_searches.py --wait 120      # seconds to wait for Atlas to index the seed

Every search goes through the repository methods (`search_by_task_name`,
`search_by_item`, `search_by_schedule_name`), so what is validated is the
exact pipeline the services will run. Exit code 1 if any check fails.
"""

from __future__ import annotations

import argparse
import pathlib
import sys
import time
from typing import Callable, List, Tuple

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from _db import connect, database_name  # noqa: E402
from seed_inventory_count_dev import DEFAULT_UMU, SEED_PREFIX, T1_SCHEDULED, TASKS_ENTITY  # noqa: E402

from pharmagob.mongodb_repositories.inventory_count_lines import (  # noqa: E402
    InventoryCountLinesRepository,
)
from pharmagob.mongodb_repositories.inventory_count_schedules import (  # noqa: E402
    InventoryCountSchedulesRepository,
)
from pharmagob.mongodb_repositories.inventory_count_tasks import (  # noqa: E402
    InventoryCountTasksRepository,
)
from pharmagob.v1.models._base import uuid_by_params  # noqa: E402
from pharmagob.v1.models.inventory_count_line import InventoryCountLineModel  # noqa: E402
from pharmagob.v1.models.inventory_count_schedule import InventoryCountScheduleModel  # noqa: E402

Check = Tuple[str, Callable[[], Tuple[bool, str]]]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--umu", default=DEFAULT_UMU)
    parser.add_argument("--db", help="database name (overrides MONGODB_DB / MONGO_DB_NAME)")
    parser.add_argument("--wait", type=int, default=180, help="seconds to wait until Atlas has indexed the seed")
    args = parser.parse_args()

    manager = connect(args.db)
    umu = args.umu
    print(f"database: {database_name(manager)} | umu: {umu}")
    tasks = InventoryCountTasksRepository(manager, TASKS_ENTITY)
    lines = InventoryCountLinesRepository(manager, InventoryCountLineModel.get_entity_name())
    schedules = InventoryCountSchedulesRepository(manager, InventoryCountScheduleModel.get_entity_name())
    db = manager.get_database()

    t1_id = uuid_by_params("SEED", umu, "T1")
    t4_id = uuid_by_params("SEED", umu, "T4")
    t1 = db[TASKS_ENTITY].find_one({"_id": t1_id})
    if not t1:
        raise SystemExit(f"T1 of the seed not found for {umu}: run seed_inventory_count_dev.py first")
    lines_col = db[InventoryCountLineModel.get_entity_name()]
    live_line = lines_col.find_one({"task.id": t1_id, "deleted_at": None, "is_in_system": True})
    deleted_line = lines_col.find_one({"task.id": t4_id, "deleted_at": {"$ne": None}})
    validation_line = lines_col.find_one({"task.id": t1_id, "status": "VALIDATION", "is_in_system": True})
    negative_line = lines_col.find_one({"task.id": t1_id, "resolution.adjustment_quantity": {"$lt": 0}})
    out_of_system_line = lines_col.find_one({"task.id": t1_id, "is_in_system": False})
    assert live_line and deleted_line and validation_line and negative_line and out_of_system_line, "seed incomplete"

    def wait_for_index() -> bool:
        """Atlas indexes new documents asynchronously: wait until the seed shows up."""
        deadline = time.time() + args.wait
        while time.time() < deadline:
            count, _ = tasks.search_by_task_name("seed", umu_id=umu)
            lines_count, _ = lines.search_by_item(live_line["item"]["id"][:7], umu_id=umu, task_id=t1_id)
            if count >= 5 and lines_count >= 1:
                return True
            time.sleep(5)
        return False

    if not wait_for_index():
        print(f"the seed is not visible through Atlas Search after {args.wait}s", file=sys.stderr)
        return 1

    def key(line: dict) -> str:
        return line["item"]["id"][:7]  # e.g. "010.000"

    def check_1():
        count, results = tasks.search_by_task_name("fisico", umu_id=umu)
        names = [r["task_name"] for r in results]
        return any("Físico" in n for n in names), f"{count} tasks: {names}"

    def check_2():
        count, results = tasks.search_by_task_name("Inv", umu_id=f"{umu}-OTHER")
        return count == 0 and not results, f"{count} tasks for another UMU"

    def check_3():
        count, results = tasks.search_by_task_name("seed", umu_id=umu, status=["OPEN", "IN_PROGRESS"])
        statuses = sorted({r["status"] for r in results})
        return bool(results) and set(statuses) <= {"OPEN", "IN_PROGRESS"} and count == len(results), f"{count} tasks, statuses {statuses}"

    def check_4():
        far = T1_SCHEDULED + 365 * 86_400_000
        _, excl = tasks.search_by_task_name("seed", umu_id=umu, scheduled_at_gt=T1_SCHEDULED, scheduled_at_lt=far)
        _, incl = tasks.search_by_task_name("seed", umu_id=umu, scheduled_at_gt=T1_SCHEDULED - 1, scheduled_at_lt=far)
        excluded = t1_id not in {r["_id"] for r in excl}
        included = t1_id in {r["_id"] for r in incl}
        return excluded and included, f"gt == scheduled_at excludes T1: {excluded}; gt - 1 includes it: {included}"

    def check_5():
        count, results = lines.search_by_item(key(live_line), umu_id=umu, task_id=t1_id)
        same_task = all(r["task"]["id"] == t1_id for r in results)
        prefix = all(r["item"]["id"].startswith(key(live_line)) or r["item"]["foreign_id"].startswith(key(live_line)) for r in results)
        ordered = [r["created_at"] for r in results] == sorted(r["created_at"] for r in results)
        return count > 0 and same_task and prefix and ordered, f"{count} lines, same task {same_task}, prefix {prefix}, created_at asc {ordered}"

    def check_6():
        count, results = lines.search_by_item(key(validation_line), umu_id=umu, status=["VALIDATION"])
        return count > 0 and all(r["status"] == "VALIDATION" for r in results), f"{count} lines, statuses {sorted({r['status'] for r in results})}"

    def check_7():
        count, results = lines.search_by_item(key(negative_line), umu_id=umu, adjustment_sign="NEGATIVE")
        quantities = [r["resolution"]["adjustment_quantity"] for r in results]
        return count > 0 and all(q < 0 for q in quantities), f"{count} lines, adjustment quantities {quantities}"

    def check_8():
        count, results = lines.search_by_item(key(deleted_line), umu_id=umu, task_id=t4_id)
        return count == 0 and not results, f"{count} lines of the cancelled task (all soft deleted)"

    def check_9():
        count, results = lines.search_by_item(key(live_line), umu_id=umu, task_id=t1_id, lot=live_line["lot"])
        return any(r["_id"] == live_line["_id"] for r in results), f"{count} lines, live line found: {live_line['_id'] in {r['_id'] for r in results}}"

    def check_10():
        count, results = lines.search_by_item(key(out_of_system_line), umu_id=umu, is_in_system=False)
        return count > 0 and all(r["is_in_system"] is False for r in results), f"{count} lines, all out of system"

    def check_11():
        count, results = schedules.search_by_schedule_name("semanal", umu_id=umu)
        return any(r["schedule_name"].startswith(SEED_PREFIX) for r in results), f"{count} schedules: {[r['schedule_name'] for r in results]}"

    def check_12():
        count_1, page_1 = lines.search_by_item("010", umu_id=umu, page=1, limit=5)
        count_2, page_2 = lines.search_by_item("010", umu_id=umu, page=2, limit=5)
        ids_1, ids_2 = {r["_id"] for r in page_1}, {r["_id"] for r in page_2}
        return count_1 == count_2 and len(page_1) == 5 and page_2 and not (ids_1 & ids_2), f"count {count_1}/{count_2}, page sizes {len(page_1)}/{len(page_2)}, disjoint {not (ids_1 & ids_2)}"

    checks: List[Check] = [
        ("1  search_by_task_name('fisico') finds 'Inventario Físico Octubre'", check_1),
        ("2  search_by_task_name('Inv', umu_id=<other>) -> 0", check_2),
        ("3  status=[OPEN, IN_PROGRESS] only, count == len(results)", check_3),
        ("4  scheduled_at range is exclusive", check_4),
        ("5  search_by_item(prefix, task_id=T1): prefix, same task, created_at asc", check_5),
        ("6  status=[VALIDATION] only", check_6),
        ("7  adjustment_sign=NEGATIVE only", check_7),
        ("8  deleted line (deleted_at numeric) does not appear", check_8),
        ("9  live line (deleted_at null) appears", check_9),
        ("10 is_in_system=False only", check_10),
        ("11 search_by_schedule_name('semanal') finds the seed schedule", check_11),
        ("12 page 2 with limit 5: same count, different results", check_12),
    ]
    failed = 0
    for title, check in checks:
        try:
            ok, detail = check()
        except Exception as error:  # a mapping error shows up here (e.g. sort on an unmapped field)
            ok, detail = False, f"{type(error).__name__}: {error}"
        failed += 0 if ok else 1
        print(f"[{'PASS' if ok else 'FAIL'}] {title}: {detail}")
    print(f"{len(checks) - failed}/{len(checks)} checks passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
