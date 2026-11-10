"""Seed the inventory count collections of DEV with documents built from the
domain models (REP-07, guide section 2.4).

    python scripts/seed_inventory_count_dev.py                      # seed the default UMU
    python scripts/seed_inventory_count_dev.py --umu MCIMB001812    # another UMU
    python scripts/seed_inventory_count_dev.py --reset              # delete what the seed created, then seed
    python scripts/seed_inventory_count_dev.py --reset --no-seed    # only delete
    python scripts/seed_inventory_count_dev.py --db other           # another database name

Connection: see `_db.py` (`MONGODB_URI` + `MONGODB_DB`, or the `MONGO_DB_*`
variables of `infra-mongodb-python`).

What it does, in order:

1. Ensures the regular indexes of `inventory_count_indexes.py` (REP-05): the
   partial unique index of D-02 and the unique `reference_id` must exist
   before the first insert.
2. Reads real data of the UMU: `location-contents` with `quantity >= 10`
   (general and FARB, via `get_batch_after`), their `items` from the catalog
   and the `locations` of the UMU. Nothing is invented: every line points to
   a real location content, item and lot. The out of system lines reuse a
   real item + lot of another UMU that does not exist in this one.
3. Builds every document with the domain models (`InventoryCountTaskModel`,
   `InventoryCountLineModel`, `InventoryCountEventModel`,
   `InventoryCountScheduleModel`) and writes them with `.dict()`. The line
   `status`, `rule.case` and `rule.note` come from the rules engine
   (`inventory_count_rules.evaluate`), round by round; the states after the
   rules (APPLYING, ADJUSTED, REJECTED, returned to audit) are applied on top
   the same way the services do it, asserting the engine left the line in
   the expected state first.
4. Folios come from `ReferenceCounterService.next_reference_id`, consumed
   only when a task is inserted for the first time.
5. Idempotent: task and schedule `_id`s are deterministic
   (`uuid_by_params("SEED", umu, key)`), line and event ids are the ones the
   domain computes, every timestamp is a fixed constant, and the writes are
   `insert_many_ignore_duplicates` (lines) and `set(write_only_if_insert)`
   (the rest). Running it twice inserts nothing new.

Inventory (`location-contents`) is never modified: the ADJUSTED lines are
documents only, no `$inc` is applied.
"""

from __future__ import annotations

import argparse
import pathlib
import sys
from dataclasses import asdict
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Sequence, Tuple

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from _db import connect, database_name  # noqa: E402

from pharmagob.mongodb_repositories.inventory_count_events import (  # noqa: E402
    InventoryCountEventsRepository,
)
from pharmagob.mongodb_repositories.inventory_count_indexes import (  # noqa: E402
    create_inventory_count_indexes,
)
from pharmagob.mongodb_repositories.inventory_count_lines import (  # noqa: E402
    InventoryCountLinesRepository,
)
from pharmagob.mongodb_repositories.inventory_count_schedules import (  # noqa: E402
    InventoryCountSchedulesRepository,
)
from pharmagob.mongodb_repositories.inventory_count_tasks import (  # noqa: E402
    InventoryCountTasksRepository,
)
from pharmagob.mongodb_repositories.items import ItemsRepository  # noqa: E402
from pharmagob.mongodb_repositories.location_contents import (  # noqa: E402
    LocationContentRepository,
)
from pharmagob.mongodb_repositories.locations import LocationRepository  # noqa: E402
from pharmagob.mongodb_repositories.reference_counters import (  # noqa: E402
    ReferenceCounterRepository,
)
from pharmagob.v1.models._base import uuid_by_params  # noqa: E402
from pharmagob.v1.models.inventory_count_event import (  # noqa: E402
    AuditActivity,
    Events,
    InventoryCountEventModel,
)
from pharmagob.v1.models.inventory_count_line import (  # noqa: E402
    InventoryCountLineModel,
    LineResolution,
    LineStatus,
    ResolutionOrigin,
    ReturnToAuditReason,
)
from pharmagob.v1.models.inventory_count_schedule import (  # noqa: E402
    Frequency,
    InventoryCountScheduleModel,
    ScheduleStatus,
)
from pharmagob.v1.models.inventory_count_task import (  # noqa: E402
    CountScope,
    CountType,
    CyclicMode,
    InventoryCountTaskModel,
    TaskStatus,
)
from pharmagob.v1.models.item import ItemModel  # noqa: E402
from pharmagob.v1.models.location import LocationModel  # noqa: E402
from pharmagob.v1.models.minified.min_models import (  # noqa: E402
    CountEntryMin,
    CountItemFilterMin,
    CountLinesQuantitiesMin,
    CountResolutionMin,
    CountRuleMin,
    ItemMin,
    LocationMin,
    UserMin,
)
from pharmagob.v1.services.inventory_count_rules import CountEntry, evaluate  # noqa: E402
from pharmagob.v1.services.reference_counters import ReferenceCounterService  # noqa: E402

DEFAULT_UMU = "908"  # the UMU of DEV (farmacias-dev) with general and FARB stock >= 10 (2026-11-03)
SEED_PREFIX = "[SEED]"
TASKS_ENTITY = InventoryCountTaskModel.get_entity_name()
MIN_QUANTITY = 10  # only location contents with quantity >= 10, so K - 9 stays positive
FARB = "FARB"

# Test users. Replace the ids with the `sub` (or email) of the Keycloak DEV
# users of each role when they exist; the README of the folder tracks this.
SEED_DOMAIN = "cenadigs.com"


def user(umu_id: str, local_part: str, display_name: str) -> UserMin:
    return UserMin(id=f"{local_part}.seed@{SEED_DOMAIN}", umu_id=umu_id, display_name=display_name)


def ts(year: int, month: int, day: int, hour: int = 0, minute: int = 0) -> int:
    """Fixed epoch ms (UTC) so every run produces the same ids."""
    return int(datetime(year, month, day, hour, minute, tzinfo=timezone.utc).timestamp() * 1000)


# Fixed timeline (UTC). 14:00 UTC = 08:00 Mexico City.
T3_SCHEDULED = ts(2026, 9, 1, 14)
T3_RELEASED = T3_SCHEDULED + 3_000
T3_R1 = ts(2026, 9, 1, 16)
T3_R2 = ts(2026, 9, 2, 16)
T3_RESOLVED = ts(2026, 9, 3, 17)
T3_CLOSED = ts(2026, 9, 5, 15)

T1_SCHEDULED = ts(2026, 10, 20, 14)
T1_RELEASED = T1_SCHEDULED + 3_200
T1_R1 = ts(2026, 10, 20, 16)  # counter 1
T1_R1B = ts(2026, 10, 20, 17, 30)  # counter 2
T1_R2 = ts(2026, 10, 21, 16)  # counter 2
T1_R2B = ts(2026, 10, 21, 17)  # counter 1
T1_AUDIT_1 = ts(2026, 10, 22, 15)
T1_AUDIT_2 = ts(2026, 10, 22, 16)
T1_RETURNED = ts(2026, 10, 22, 18)
T1_AUDIT_3 = ts(2026, 10, 23, 15)
T1_RESOLVED = ts(2026, 10, 23, 17)
T1_APPLYING = ts(2026, 10, 23, 17, 30)

T4_SCHEDULED = ts(2026, 10, 27, 15)
T4_RELEASED = T4_SCHEDULED + 2_500
T4_CANCELLED = ts(2026, 10, 27, 18)

S1_STARTS = ts(2026, 10, 27, 14)
T2_SCHEDULED = ts(2026, 11, 3, 14)
T2_RELEASED = T2_SCHEDULED + 2_800
S1_NEXT_RUN = ts(2026, 11, 10, 14)

T5_SCHEDULED = ts(2026, 11, 24, 14)

# T6: a cyclic (MANUAL) task resolved end to end, so CU-48 can be checked in
# the CYCLIC modality (adjustments and a rejection with count_type CYCLIC).
T6_SCHEDULED = ts(2026, 10, 29, 14)
T6_RELEASED = T6_SCHEDULED + 2_600
T6_R1 = ts(2026, 10, 29, 16)
T6_R2 = ts(2026, 10, 30, 16)
T6_AUDIT = ts(2026, 10, 31, 15)
T6_RESOLVED = ts(2026, 10, 31, 17)
T6_CLOSED = ts(2026, 10, 31, 18)


# --------------------------------------------------------------------------- helpers


def to_doc(model) -> dict:
    """`.dict()` with enum members replaced by their values (what the services store)."""

    def plain(value: Any) -> Any:
        if isinstance(value, Enum):
            return value.value
        if isinstance(value, dict):
            return {key: plain(item) for key, item in value.items()}
        if isinstance(value, list):
            return [plain(item) for item in value]
        return value

    return plain(asdict(model))


class Seed:
    def __init__(self, manager, umu_id: str) -> None:
        self.manager = manager
        self.umu_id = umu_id
        self.tasks_repo = InventoryCountTasksRepository(manager, TASKS_ENTITY)
        self.lines_repo = InventoryCountLinesRepository(
            manager, InventoryCountLineModel.get_entity_name()
        )
        self.events_repo = InventoryCountEventsRepository(
            manager, InventoryCountEventModel.get_entity_name()
        )
        self.schedules_repo = InventoryCountSchedulesRepository(
            manager, InventoryCountScheduleModel.get_entity_name()
        )
        self.location_contents_repo = LocationContentRepository(manager, "location-contents")
        self.items_repo = ItemsRepository(manager, ItemModel.get_entity_name())
        self.locations_repo = LocationRepository(manager, LocationModel.get_entity_name())
        self.references = ReferenceCounterService(ReferenceCounterRepository(manager))

        self.planner = user(umu_id, "planeador", "Planeador (seed)")
        self.counter_1 = user(umu_id, "contador", "Contador (seed)")
        self.counter_2 = user(umu_id, "contador2", "Contador 2 (seed)")
        self.auditor = user(umu_id, "auditor", "Auditor (seed)")

        self.tasks: List[InventoryCountTaskModel] = []
        self.lines: List[InventoryCountLineModel] = []
        self.events: List[InventoryCountEventModel] = []
        self.schedules: List[InventoryCountScheduleModel] = []
        self._items: Dict[str, ItemMin] = {}
        self._existing_tasks: Dict[str, dict] = {}

    # ---- real data of the UMU

    def load_inventory(self) -> None:
        count, locations = self.locations_repo.get_by_umu_id(self.umu_id)
        locations = [LocationModel(**doc) for doc in locations]
        by_label = {location.label_code: location for location in locations if not location.disabled}
        if FARB not in by_label or "STANDARD" not in by_label:
            raise SystemExit(
                f"UMU {self.umu_id} needs a STANDARD and a FARB location; found {sorted(by_label)}"
            )
        self.standard_location = by_label["STANDARD"].minified()
        self.farb_location = by_label[FARB].minified()

        self.general = self.location_contents_repo.get_batch_after(
            self.umu_id,
            limit=200,
            location_label_code_not_in=[FARB],
            quantity_gt=MIN_QUANTITY - 1,
        )
        self.farb = self.location_contents_repo.get_batch_after(
            self.umu_id, limit=50, location_label_code=FARB, quantity_gt=MIN_QUANTITY - 1
        )
        needed_general, needed_farb = 30 + 12 + 5 + 5, 10 + 3
        if len(self.general) < needed_general or len(self.farb) < needed_farb:
            raise SystemExit(
                f"UMU {self.umu_id} has {len(self.general)} general and {len(self.farb)} FARB "
                f"location contents with quantity >= {MIN_QUANTITY}; the seed needs "
                f"{needed_general} and {needed_farb}. Pick another UMU with --umu."
            )
        # Disjoint slices so no location content appears in two tasks.
        self.t1_general = self.general[0:30]
        self.t3_general = self.general[30:42]
        self.t4_general = self.general[42:47]
        self.t6_general = self.general[47:52]
        self.t2_farb = self.farb[0:10]
        self.t3_farb = self.farb[10:13]
        self.out_of_system = self._out_of_system_candidates(4)  # T1 uses four
        print(
            f"UMU {self.umu_id}: {len(self.general)} general + {len(self.farb)} FARB location "
            f"contents with quantity >= {MIN_QUANTITY}; {len(self.out_of_system)} out of system keys"
        )

    def _out_of_system_candidates(self, how_many: int) -> List[dict]:
        """Real item + lot pairs of other UMUs whose location content does not
        exist in the STANDARD location of this UMU."""
        found: List[dict] = []
        seen_items = set()
        page = 1
        while len(found) < how_many and page <= 20:
            _, batch = self.location_contents_repo.get_paginated(
                page,
                50,
                and_conditions=[("umu_id", "not in", [self.umu_id]), ("quantity", ">", 0)],
                sort=[("_id", 1)],
            )
            batch = list(batch)
            if not batch:
                break
            for candidate in batch:
                item_id = candidate["item"]["id"]
                if item_id in seen_items:
                    continue
                location_content_id = uuid_by_params(
                    item_id, candidate["lot"], self.standard_location.id
                )
                if self.location_contents_repo.get(location_content_id) is None:
                    seen_items.add(item_id)
                    found.append(candidate)
                    if len(found) == how_many:
                        break
            page += 1
        if len(found) < how_many:
            raise SystemExit("could not find enough out of system item/lot pairs")
        return found

    def item(self, item_id: str) -> ItemMin:
        if item_id not in self._items:
            doc = self.items_repo.get(item_id)
            if doc is None:
                raise SystemExit(f"item {item_id} not in the catalog")
            self._items[item_id] = ItemModel(**doc).minified()
        return self._items[item_id]

    # ---- builders

    def task(
        self,
        key: str,
        name: str,
        *,
        count_type: CountType,
        count_scope: CountScope,
        status: TaskStatus,
        scheduled_at: int,
        released_at: Optional[int] = None,
        cyclic_mode: Optional[CyclicMode] = None,
        item_filter: Optional[CountItemFilterMin] = None,
        schedule=None,
        created_at: Optional[int] = None,
        updated_at: Optional[int] = None,
    ) -> InventoryCountTaskModel:
        task_id = uuid_by_params("SEED", self.umu_id, key)
        existing = self.tasks_repo.get(task_id)
        if existing:
            self._existing_tasks[task_id] = existing
            reference_id = existing["reference_id"]
        else:
            prefix = "CF" if count_type is CountType.PHYSICAL else "CC"
            reference_id = self.references.next_reference_id(TASKS_ENTITY, prefix)
        task = InventoryCountTaskModel(
            _id=task_id,
            umu_id=self.umu_id,
            reference_id=reference_id,
            task_name=f"{SEED_PREFIX} {name}",
            count_type=count_type,
            cyclic_mode=cyclic_mode,
            count_scope=count_scope,
            item_filter=item_filter,
            is_zero_stock_included=False,
            scheduled_at=scheduled_at,
            released_at=released_at,
            status=status,
            schedule=schedule,
            author=self.planner,
            last_author=self.planner,
            created_at=created_at or scheduled_at,
            updated_at=updated_at or released_at or scheduled_at,
        )
        self.tasks.append(task)
        self.event(
            Events.TASK_SCHEDULED,
            task,
            self.planner,
            AuditActivity.CREATION,
            scheduled_at,
            context={
                "count_scope": count_scope.value,
                "is_zero_stock_included": False,
                "item_ids_quantity": len(item_filter.item_ids) if item_filter and item_filter.item_ids else None,
            },
        )
        return task

    def line(
        self,
        task: InventoryCountTaskModel,
        location_content: dict,
        *,
        created_at: int,
        author: Optional[UserMin] = None,
    ) -> InventoryCountLineModel:
        """A PENDING line over a real location content (in system)."""
        line = InventoryCountLineModel(
            umu_id=self.umu_id,
            task=task.minified(),
            item=self.item(location_content["item"]["id"]),
            lot=location_content["lot"],
            expiration_date=location_content["expiration_date"],
            location=LocationMin(**location_content["location"]),
            location_content_id=location_content["_id"],
            is_in_system=True,
            kardex_initial_quantity=location_content["quantity"],
            last_author=author or self.planner,
            created_at=created_at,
            updated_at=created_at,
        )
        self.lines.append(line)
        return line

    def out_of_system_line(
        self, task: InventoryCountTaskModel, candidate: dict, *, counted_at: int, author: UserMin
    ) -> InventoryCountLineModel:
        """A key found on the shelf that has no location content in this UMU
        (D-04): `location_content_id` is computed with the same formula."""
        location = self.standard_location
        line = InventoryCountLineModel(
            umu_id=self.umu_id,
            task=task.minified(),
            item=self.item(candidate["item"]["id"]),
            lot=candidate["lot"],
            expiration_date=candidate["expiration_date"],
            location=location,
            location_content_id=uuid_by_params(candidate["item"]["id"], candidate["lot"], location.id),
            is_in_system=False,
            kardex_initial_quantity=0,
            last_author=author,
            created_at=counted_at,
            updated_at=counted_at,
        )
        self.lines.append(line)
        self.event(
            Events.OUT_OF_SYSTEM_REGISTERED,
            task,
            author,
            AuditActivity.CREATION,
            counted_at,
            line=line,
            context={"item_id": line.item.id, "lot": line.lot, "location_id": location.id},
        )
        return line

    def count(
        self,
        line: InventoryCountLineModel,
        counted: int,
        *,
        author: UserMin,
        counted_at: int,
        snapshot: Optional[int] = None,
    ) -> None:
        """Save one round and let the rules engine decide status and case.

        `snapshot` is the location content quantity at that moment; by default
        the initial Kardex. Pass a different one to simulate a movement
        between rounds (D-01: the engine compares differences)."""
        round_ = len(line.counts) + 1
        entry = CountEntryMin(
            round=round_,
            counted_quantity=counted,
            kardex_snapshot_quantity=line.kardex_initial_quantity if snapshot is None else snapshot,
            author=author,
            counted_at=counted_at,
        )
        line.counts.append(entry)
        result = evaluate(
            [CountEntry(c.counted_quantity, c.kardex_snapshot_quantity) for c in line.counts],
            is_in_system=line.is_in_system,
        )
        line.status = result.status
        case = result.case if result.case is not None else (line.rule.case if line.rule else None)
        line.rule = CountRuleMin(case=case, note=result.note)
        # H-28: the signed difference still to apply lives in a flat field while the
        # line waits in ADJUSTMENT, so Adjustments can filter by sign (resolution is None).
        line.pending_adjustment_quantity = (
            entry.counted_quantity - entry.kardex_snapshot_quantity
            if result.status is LineStatus.ADJUSTMENT
            else None
        )
        line.last_author = author
        line.updated_at = counted_at
        if result.status is LineStatus.CLOSED:
            self._resolve(
                line,
                LineResolution.NO_DIFFERENCE,
                ResolutionOrigin.RULES,
                author=author,
                resolved_at=counted_at,
                adjustment_quantity=0,
                final_quantity=counted,
            )

    def validated_count(
        self, line: InventoryCountLineModel, counted: int, *, counted_at: int, snapshot: Optional[int] = None
    ) -> None:
        """Round 3+ captured and validated by the auditor (AUDIT_VALIDATED)."""
        assert line.status is LineStatus.VALIDATION, f"{line.item.id}: expected VALIDATION, got {line.status}"
        self.count(line, counted, author=self.auditor, counted_at=counted_at, snapshot=snapshot)
        self.event(
            Events.AUDIT_VALIDATED,
            self._task_of(line),
            self.auditor,
            AuditActivity.MODIFICATION,
            counted_at,
            line=line,
            initial_quantity=line.counts[-1].kardex_snapshot_quantity,
            final_quantity=counted,
            context={"round": line.counts[-1].round, "counted_at": counted_at, "rule_case": line.rule.case},
        )

    def adjust(self, line: InventoryCountLineModel, *, resolved_at: int) -> None:
        """ADJUSTMENT -> ADJUSTED. The adjustment is the difference of the last
        round; `final_quantity` is what was counted."""
        assert line.status is LineStatus.ADJUSTMENT, f"{line.item.id}: expected ADJUSTMENT, got {line.status}"
        last = line.counts[-1]
        delta = last.counted_quantity - last.kardex_snapshot_quantity
        line.status = LineStatus.ADJUSTED
        self._resolve(
            line,
            LineResolution.ADJUSTED,
            ResolutionOrigin.ADJUSTMENTS,
            author=self.auditor,
            resolved_at=resolved_at,
            adjustment_quantity=delta,
            final_quantity=last.counted_quantity,
        )
        self.event(
            Events.ADJUSTMENT_POSITIVE if delta > 0 else Events.ADJUSTMENT_NEGATIVE,
            self._task_of(line),
            self.auditor,
            AuditActivity.MODIFICATION,
            resolved_at,
            line=line,
            initial_quantity=last.kardex_snapshot_quantity,
            final_quantity=last.counted_quantity,
            comment=line.rule.note,
            context={
                "rule_case": line.rule.case,
                "adjustment_quantity": delta,
                "resolution_origin": ResolutionOrigin.ADJUSTMENTS.value,
                "is_in_system": line.is_in_system,
                "seed_note": "document only: location-contents was not modified by the seed",
            },
        )

    def applying(self, line: InventoryCountLineModel, *, resolved_at: int) -> None:
        """ADJUSTMENT -> APPLYING (claim of ADJ-03, H-21). Written with the whole
        `resolution` so the document is readable with `Model(**doc)`.
        `pending_adjustment_quantity` is kept: the line is still pending for P-12 (H-28)."""
        assert line.status is LineStatus.ADJUSTMENT, f"{line.item.id}: expected ADJUSTMENT, got {line.status}"
        last = line.counts[-1]
        line.status = LineStatus.APPLYING
        line.resolution = CountResolutionMin(
            type=LineResolution.ADJUSTED.value,
            origin=ResolutionOrigin.ADJUSTMENTS.value,
            author=self.auditor,
            resolved_at=resolved_at,
            adjustment_quantity=last.counted_quantity - last.kardex_snapshot_quantity,
            final_quantity=last.counted_quantity,
        )
        line.last_author = self.auditor
        line.updated_at = resolved_at

    def reject(self, line: InventoryCountLineModel, *, resolved_at: int, origin: ResolutionOrigin) -> None:
        """VALIDATION -> REJECTED (origin AUDIT) or ADJUSTMENT -> REJECTED (origin
        ADJUSTMENTS): `adjustment_quantity` 0, `final_quantity` = Kardex."""
        expected = LineStatus.VALIDATION if origin is ResolutionOrigin.AUDIT else LineStatus.ADJUSTMENT
        assert line.status is expected, f"{line.item.id}: expected {expected}, got {line.status}"
        last = line.counts[-1]
        line.status = LineStatus.REJECTED
        self._resolve(
            line,
            LineResolution.REJECTED,
            origin,
            author=self.auditor,
            resolved_at=resolved_at,
            adjustment_quantity=0,
            final_quantity=last.kardex_snapshot_quantity,
        )
        self.event(
            Events.REJECTED,
            self._task_of(line),
            self.auditor,
            AuditActivity.MODIFICATION,
            resolved_at,
            line=line,
            initial_quantity=last.kardex_snapshot_quantity,
            final_quantity=last.kardex_snapshot_quantity,
            comment="Falso positivo: los conteos no reflejan la existencia real",
            context={"rule_case": line.rule.case, "adjustment_quantity": 0, "resolution_origin": origin.value},
        )

    def return_to_audit(
        self,
        line: InventoryCountLineModel,
        *,
        reason: ReturnToAuditReason,
        returned_at: int,
        current_quantity: int,
    ) -> None:
        """ADJUSTMENT/APPLYING -> VALIDATION (H-13 insufficient stock, H-15 sent by
        the auditor). `resolved_at` is cleared, `resolution` is not written."""
        assert line.status in (LineStatus.ADJUSTMENT, LineStatus.APPLYING), f"{line.item.id}: {line.status}"
        last = line.counts[-1]
        attempted = last.counted_quantity - last.kardex_snapshot_quantity
        line.status = LineStatus.VALIDATION
        line.resolution = None
        line.pending_adjustment_quantity = None  # no longer pending to apply (H-28)
        line.is_open = True
        line.context = {
            **(line.context or {}),
            "returned_to_audit": {
                "reason": reason.value,
                "current_quantity": current_quantity,
                "attempted_adjustment_quantity": attempted,
                "returned_at": returned_at,
            },
        }
        line.last_author = self.auditor
        line.updated_at = returned_at
        self.event(
            Events.ADJUSTMENT_RETURNED_TO_AUDIT,
            self._task_of(line),
            self.auditor,
            AuditActivity.MODIFICATION,
            returned_at,
            line=line,
            initial_quantity=current_quantity,
            final_quantity=current_quantity,
            context={"returned_to_audit": line.context["returned_to_audit"], "rule_case": line.rule.case},
        )

    def _resolve(
        self,
        line: InventoryCountLineModel,
        type_: LineResolution,
        origin: ResolutionOrigin,
        *,
        author: UserMin,
        resolved_at: int,
        adjustment_quantity: int,
        final_quantity: int,
    ) -> None:
        line.resolution = CountResolutionMin(
            type=type_.value,
            origin=origin.value,
            author=author,
            resolved_at=resolved_at,
            adjustment_quantity=adjustment_quantity,
            final_quantity=final_quantity,
        )
        line.pending_adjustment_quantity = None  # applied or rejected: the truth is in resolution (H-28)
        line.is_open = False
        line.last_author = author
        line.updated_at = resolved_at

    def soft_delete(self, line: InventoryCountLineModel, *, deleted_at: int) -> None:
        line.deleted_at = deleted_at
        line.is_open = False
        line.updated_at = deleted_at

    def captured(self, task: InventoryCountTaskModel, author: UserMin, counted_at: int, lines_quantity: int, round_: int) -> None:
        """One COUNT_CAPTURED per save (author + counted_at), not per line."""
        self.event(
            Events.COUNT_CAPTURED,
            task,
            author,
            AuditActivity.MODIFICATION,
            counted_at,
            context={"round": round_, "counted_at": counted_at, "lines_quantity": lines_quantity},
        )

    def event(
        self,
        event: Events,
        task: Optional[InventoryCountTaskModel],
        author: UserMin,
        activity: AuditActivity,
        at: int,
        *,
        line: Optional[InventoryCountLineModel] = None,
        schedule: Optional[InventoryCountScheduleModel] = None,
        initial_quantity: Optional[int] = None,
        final_quantity: Optional[int] = None,
        comment: Optional[str] = None,
        context: Optional[dict] = None,
    ) -> InventoryCountEventModel:
        count_type = task.count_type if task else CountType.CYCLIC
        model = InventoryCountEventModel(
            umu_id=self.umu_id,
            author=author,
            activity=activity,
            count_type=count_type,
            task=task.minified() if task else None,
            line=line.minified() if line else None,
            schedule=schedule.minified() if schedule else None,
            initial_quantity=initial_quantity,
            final_quantity=final_quantity,
            comment=comment,
            context={key: value for key, value in (context or {}).items() if value is not None},
            transition_timestamp=at,
            event=event,
            created_at=at,
        )
        self.events.append(model)
        return model

    def _task_of(self, line: InventoryCountLineModel) -> InventoryCountTaskModel:
        return next(task for task in self.tasks if task._id == line.task.id)

    def close_task_counters(self, task: InventoryCountTaskModel) -> None:
        # The counters are set at release and keep the soft deleted lines of a
        # cancelled task; resolved = closed with a resolution (APPLYING is still open).
        lines = [line for line in self.lines if line.task.id == task._id]
        task.lines = CountLinesQuantitiesMin(
            total_quantity=len(lines),
            counted_quantity=sum(1 for line in lines if line.counts),
            resolved_quantity=sum(1 for line in lines if not line.is_open and line.deleted_at is None),
        )

    # ---- the scenarios

    def build(self) -> None:
        self.build_t3_closed_september()
        self.build_t1_physical_in_progress()
        self.build_t4_cancelled()
        self.build_s1_schedule_and_t2()
        self.build_t5_scheduled()
        self.build_t6_cyclic_resolved()
        for task in self.tasks:
            self.close_task_counters(task)

    def build_t1_physical_in_progress(self) -> None:
        t1 = self.task(
            "T1",
            "Inventario Físico Octubre",
            count_type=CountType.PHYSICAL,
            count_scope=CountScope.GENERAL,
            status=TaskStatus.IN_PROGRESS,
            scheduled_at=T1_SCHEDULED,
            released_at=T1_RELEASED,
            updated_at=T1_APPLYING,
        )
        lcs = iter(self.t1_general)
        oos = iter(self.out_of_system)
        c1, c2, aud = self.counter_1, self.counter_2, self.auditor
        line = lambda: self.line(t1, next(lcs), created_at=T1_RELEASED)  # noqa: E731
        K = lambda ln: ln.kardex_initial_quantity  # noqa: E731

        # 6 PENDING: no counts, rule stays None (scenario 1 of the epic).
        for _ in range(6):
            line()

        # 5 SECOND_COUNT: cases 3, 3, 9, 10 and one out of system (case 5).
        ln = line(); self.count(ln, K(ln) - 2, author=c1, counted_at=T1_R1)  # case 3
        ln = line(); self.count(ln, K(ln) - 1, author=c1, counted_at=T1_R1)  # case 3
        ln = line(); self.count(ln, 0, author=c1, counted_at=T1_R1)  # case 9
        ln = line(); self.count(ln, K(ln) + 3, author=c2, counted_at=T1_R1B)  # case 10
        ln = self.out_of_system_line(t1, next(oos), counted_at=T1_R1B, author=c2)
        self.count(ln, 150, author=c2, counted_at=T1_R1B)  # case 5

        # 4 VALIDATION: case 12 (out of system), case 14, and two returned from adjustment.
        ln = self.out_of_system_line(t1, next(oos), counted_at=T1_R1, author=c1)
        self.count(ln, 150, author=c1, counted_at=T1_R1)
        self.count(ln, 120, author=c2, counted_at=T1_R2)  # case 12
        ln = line(); self.count(ln, 0, author=c1, counted_at=T1_R1); self.count(ln, 10, author=c2, counted_at=T1_R2)  # case 14
        ln = line(); self.count(ln, K(ln) - 5, author=c1, counted_at=T1_R1); self.count(ln, K(ln) - 5, author=c2, counted_at=T1_R2)  # case 2
        self.applying(ln, resolved_at=T1_RESOLVED)
        self.return_to_audit(ln, reason=ReturnToAuditReason.INSUFFICIENT_STOCK, returned_at=T1_RETURNED, current_quantity=3)  # H-13
        ln = line(); self.count(ln, K(ln) + 2, author=c1, counted_at=T1_R1); self.count(ln, K(ln) + 2, author=c2, counted_at=T1_R2)  # case 10
        self.return_to_audit(ln, reason=ReturnToAuditReason.SENT_BY_AUDITOR, returned_at=T1_RETURNED, current_quantity=K(ln) - 4)  # H-15

        # 4 ADJUSTMENT: cases 2, 4, 10 confirmed, 9 confirmed (waiting for the adjustment screen).
        ln = line(); self.count(ln, K(ln) - 5, author=c1, counted_at=T1_R1); self.count(ln, K(ln) - 5, author=c2, counted_at=T1_R2)  # case 2
        ln = self.out_of_system_line(t1, next(oos), counted_at=T1_R1, author=c1)
        self.count(ln, 150, author=c1, counted_at=T1_R1); self.count(ln, 150, author=c2, counted_at=T1_R2)  # case 4
        ln = line(); self.count(ln, K(ln) + 3, author=c1, counted_at=T1_R1); self.count(ln, K(ln) + 3, author=c2, counted_at=T1_R2)  # case 10
        ln = line(); self.count(ln, 0, author=c1, counted_at=T1_R1); self.count(ln, 0, author=c2, counted_at=T1_R2)  # case 9

        # 1 APPLYING: claimed by the adjustment, resolved_at fixed (H-21).
        ln = line(); self.count(ln, K(ln) - 4, author=c1, counted_at=T1_R1); self.count(ln, K(ln) - 4, author=c2, counted_at=T1_R2)
        self.applying(ln, resolved_at=T1_APPLYING)

        # 5 CLOSED: cases 7 (x3) and 13 (x2).
        for _ in range(3):
            ln = line(); self.count(ln, K(ln), author=c1, counted_at=T1_R1)  # case 7
        for _ in range(2):
            ln = line(); self.count(ln, K(ln) - 1, author=c2, counted_at=T1_R1B); self.count(ln, K(ln), author=c1, counted_at=T1_R2B)  # case 13

        # 3 ADJUSTED: positive (through audit), negative (D-01 movement, returned once,
        # validated twice) and out of system.
        ln = line(); self.count(ln, K(ln) + 4, author=c1, counted_at=T1_R1); self.count(ln, K(ln) + 2, author=c2, counted_at=T1_R2)  # case 14
        self.validated_count(ln, K(ln) + 4, counted_at=T1_AUDIT_1)  # round 3 -> ADJUSTMENT
        self.adjust(ln, resolved_at=T1_RESOLVED)  # +4
        ln = line(); k = K(ln)
        self.count(ln, k - 5, author=c1, counted_at=T1_R1)  # diff -5
        self.count(ln, k - 8, author=c2, counted_at=T1_R2)  # diff -8 -> case 14, audit
        self.validated_count(ln, k - 5, counted_at=T1_AUDIT_2)  # round 3, diff -5 -> ADJUSTMENT
        self.return_to_audit(ln, reason=ReturnToAuditReason.INSUFFICIENT_STOCK, returned_at=T1_RETURNED, current_quantity=k - 4)
        self.validated_count(ln, k - 9, counted_at=T1_AUDIT_3, snapshot=k - 4)  # round 4, diff -5 again (D-01)
        self.adjust(ln, resolved_at=T1_RESOLVED)  # -5
        # This line keeps context.returned_to_audit as history of the H-13 round trip.
        ln = self.out_of_system_line(t1, next(oos), counted_at=T1_R1, author=c1)
        self.count(ln, 24, author=c1, counted_at=T1_R1); self.count(ln, 24, author=c2, counted_at=T1_R2)  # case 4
        self.adjust(ln, resolved_at=T1_RESOLVED)  # +24, D-04 would create the location content

        # 2 REJECTED: from audit (case 14) and from adjustments (case 2).
        ln = line(); self.count(ln, 0, author=c1, counted_at=T1_R1); self.count(ln, 5, author=c2, counted_at=T1_R2)  # case 14
        self.reject(ln, resolved_at=T1_RESOLVED, origin=ResolutionOrigin.AUDIT)
        ln = line(); self.count(ln, K(ln) - 3, author=c1, counted_at=T1_R1); self.count(ln, K(ln) - 3, author=c2, counted_at=T1_R2)  # case 2
        self.reject(ln, resolved_at=T1_RESOLVED, origin=ResolutionOrigin.ADJUSTMENTS)

        t1_lines = [line_ for line_ in self.lines if line_.task.id == t1._id]
        assert len(t1_lines) == 30, len(t1_lines)
        self.event(
            Events.TASK_RELEASED,
            t1,
            self.planner,
            AuditActivity.MODIFICATION,
            T1_RELEASED,
            context={
                "total_quantity": sum(1 for line_ in t1_lines if line_.is_in_system),
                "omitted_quantity": 0,
                "is_zero_stock_included": False,
            },
        )
        # One COUNT_CAPTURED per save: counter 1 and counter 2, rounds 1 and 2.
        for author, at, round_ in ((c1, T1_R1, 1), (c2, T1_R1B, 1), (c2, T1_R2, 2), (c1, T1_R2B, 2)):
            quantity = sum(1 for line_ in t1_lines for c in line_.counts if c.counted_at == at and c.author.id == author.id)
            self.captured(t1, author, at, quantity, round_)

    def build_t3_closed_september(self) -> None:
        t3 = self.task(
            "T3",
            "Físico cerrado septiembre",
            count_type=CountType.PHYSICAL,
            count_scope=CountScope.ALL,
            status=TaskStatus.CLOSED,
            scheduled_at=T3_SCHEDULED,
            released_at=T3_RELEASED,
            updated_at=T3_CLOSED,
        )
        lcs = iter(self.t3_general + self.t3_farb)
        c1, c2 = self.counter_1, self.counter_2
        line = lambda: self.line(t3, next(lcs), created_at=T3_RELEASED)  # noqa: E731
        K = lambda ln: ln.kardex_initial_quantity  # noqa: E731
        for _ in range(9):  # case 7
            ln = line(); self.count(ln, K(ln), author=c1, counted_at=T3_R1)
        for _ in range(2):  # case 13
            ln = line(); self.count(ln, K(ln) + 1, author=c2, counted_at=T3_R1); self.count(ln, K(ln), author=c1, counted_at=T3_R2)
        ln = line(); self.count(ln, K(ln) + 2, author=c1, counted_at=T3_R1); self.count(ln, K(ln) + 2, author=c2, counted_at=T3_R2)
        self.adjust(ln, resolved_at=T3_RESOLVED)  # +2
        ln = line(); self.count(ln, K(ln) - 3, author=c1, counted_at=T3_R1); self.count(ln, K(ln) - 3, author=c2, counted_at=T3_R2)
        self.adjust(ln, resolved_at=T3_RESOLVED)  # -3
        ln = line(); self.count(ln, 0, author=c1, counted_at=T3_R1); self.count(ln, 4, author=c2, counted_at=T3_R2)  # case 14
        self.reject(ln, resolved_at=T3_RESOLVED, origin=ResolutionOrigin.AUDIT)
        ln = line(); self.count(ln, K(ln) - 6, author=c1, counted_at=T3_R1); self.count(ln, K(ln) - 6, author=c2, counted_at=T3_R2)  # case 2
        self.reject(ln, resolved_at=T3_RESOLVED, origin=ResolutionOrigin.ADJUSTMENTS)
        t3_lines = [line_ for line_ in self.lines if line_.task.id == t3._id]
        assert len(t3_lines) == 15 and all(not line_.is_open for line_ in t3_lines)
        self.event(
            Events.TASK_RELEASED, t3, self.planner, AuditActivity.MODIFICATION, T3_RELEASED,
            context={"total_quantity": 15, "omitted_quantity": 0, "is_zero_stock_included": False},
        )
        for author, at, round_ in ((c1, T3_R1, 1), (c2, T3_R1, 1), (c2, T3_R2, 2), (c1, T3_R2, 2)):
            quantity = sum(1 for line_ in t3_lines for c in line_.counts if c.counted_at == at and c.author.id == author.id)
            if quantity:
                self.captured(t3, author, at, quantity, round_)
        self.event(
            Events.TASK_CLOSED, t3, self.auditor, AuditActivity.MODIFICATION, T3_CLOSED,
            context={"resolved_quantity": 15, "adjusted_quantity": 2, "rejected_quantity": 2},
        )

    def build_t4_cancelled(self) -> None:
        item_ids = sorted({lc["item"]["id"] for lc in self.t4_general})
        t4 = self.task(
            "T4",
            "Cancelada sin capturas",
            count_type=CountType.CYCLIC,
            cyclic_mode=CyclicMode.MANUAL,
            count_scope=CountScope.GENERAL,
            status=TaskStatus.CANCELLED,
            scheduled_at=T4_SCHEDULED,
            released_at=T4_RELEASED,
            item_filter=CountItemFilterMin(item_ids=item_ids),
            updated_at=T4_CANCELLED,
        )
        for lc in self.t4_general:
            self.soft_delete(self.line(t4, lc, created_at=T4_RELEASED), deleted_at=T4_CANCELLED)
        self.event(
            Events.TASK_RELEASED, t4, self.planner, AuditActivity.MODIFICATION, T4_RELEASED,
            context={"total_quantity": 5, "omitted_quantity": 0, "is_zero_stock_included": False, "items_without_stock": []},
        )
        self.event(
            Events.TASK_CANCELLED, t4, self.planner, AuditActivity.DELETION, T4_CANCELLED,
            comment="Cancelada por el Planeador antes de capturar",
            context={"lines_deleted_quantity": 5},
        )

    def build_s1_schedule_and_t2(self) -> None:
        farb_item_ids = sorted({lc["item"]["id"] for lc in self.t2_farb})
        all_farb_items = {lc["item"]["id"] for lc in self.farb}
        without_stock = sorted({lc["item"]["id"] for lc in self.general} - all_farb_items)[:2]
        item_filter = CountItemFilterMin(item_ids=farb_item_ids + without_stock)

        schedule = InventoryCountScheduleModel(
            _id=uuid_by_params("SEED", self.umu_id, "S1"),
            umu_id=self.umu_id,
            schedule_name=f"{SEED_PREFIX} Semanal bienestar",
            frequency=Frequency.WEEKLY,
            run_time="08:00",
            day_of_week=2,  # tuesday, like T2_SCHEDULED
            starts_at=S1_STARTS,
            count_scope=CountScope.BIENESTAR,
            item_filter=item_filter,
            task_validity_days=7,
            status=ScheduleStatus.ACTIVE,
            next_run_at=S1_NEXT_RUN,
            last_run_at=T2_SCHEDULED,
            author=self.planner,
            last_author=self.planner,
            created_at=S1_STARTS,
            updated_at=T2_RELEASED,
        )
        self.schedules.append(schedule)
        self.event(
            Events.SCHEDULE_CREATED, None, self.planner, AuditActivity.CREATION, S1_STARTS,
            schedule=schedule,
            context={"frequency": Frequency.WEEKLY.value, "next_run_at": S1_STARTS + 7 * 86_400_000, "count_scope": CountScope.BIENESTAR.value},
        )
        # T2 is the run of S1 (cyclic_mode SCHEDULED, schedule set, S1.last_task = T2).
        t2 = self.task(
            "T2",
            "Cíclico controlados",
            count_type=CountType.CYCLIC,
            cyclic_mode=CyclicMode.SCHEDULED,
            count_scope=CountScope.BIENESTAR,
            status=TaskStatus.OPEN,
            scheduled_at=T2_SCHEDULED,
            released_at=T2_RELEASED,
            item_filter=item_filter,
            schedule=schedule.minified(),
        )
        schedule.last_task = t2.minified()
        for lc in self.t2_farb:
            self.line(t2, lc, created_at=T2_RELEASED)
        self.event(
            Events.TASK_RELEASED, t2, self.planner, AuditActivity.MODIFICATION, T2_RELEASED,
            context={"total_quantity": 10, "omitted_quantity": 0, "is_zero_stock_included": False, "items_without_stock": without_stock},
        )

    def build_t5_scheduled(self) -> None:
        self.task(
            "T5",
            "Programado noviembre",
            count_type=CountType.PHYSICAL,
            count_scope=CountScope.GENERAL,
            status=TaskStatus.SCHEDULED,
            scheduled_at=T5_SCHEDULED,
            created_at=ts(2026, 11, 3, 15),
            updated_at=ts(2026, 11, 3, 15),
        )

    def build_t6_cyclic_resolved(self) -> None:
        """CU-48 in the CYCLIC modality: a manual cyclic task, closed, with one
        positive adjustment (through audit), one negative adjustment, one
        rejection and two lines closed by the rules. Every event of this task
        carries `count_type = CYCLIC`."""
        item_ids = sorted({lc["item"]["id"] for lc in self.t6_general})
        t6 = self.task(
            "T6",
            "Cíclico resuelto",
            count_type=CountType.CYCLIC,
            cyclic_mode=CyclicMode.MANUAL,
            count_scope=CountScope.GENERAL,
            status=TaskStatus.CLOSED,
            scheduled_at=T6_SCHEDULED,
            released_at=T6_RELEASED,
            item_filter=CountItemFilterMin(item_ids=item_ids),
            updated_at=T6_CLOSED,
        )
        lcs = iter(self.t6_general)
        c1, c2 = self.counter_1, self.counter_2
        line = lambda: self.line(t6, next(lcs), created_at=T6_RELEASED)  # noqa: E731
        K = lambda ln: ln.kardex_initial_quantity  # noqa: E731

        # ADJUSTMENT_POSITIVE through audit: rounds differ (case 14), the auditor validates +3.
        ln = line(); self.count(ln, K(ln) + 3, author=c1, counted_at=T6_R1); self.count(ln, K(ln) + 1, author=c2, counted_at=T6_R2)  # case 14
        self.validated_count(ln, K(ln) + 3, counted_at=T6_AUDIT)  # round 3 -> ADJUSTMENT
        self.adjust(ln, resolved_at=T6_RESOLVED)  # +3
        # ADJUSTMENT_NEGATIVE confirmed by the two counters (case 2).
        ln = line(); self.count(ln, K(ln) - 4, author=c1, counted_at=T6_R1); self.count(ln, K(ln) - 4, author=c2, counted_at=T6_R2)  # case 2
        self.adjust(ln, resolved_at=T6_RESOLVED)  # -4
        # REJECTED from audit (false positive, case 14).
        ln = line(); self.count(ln, 0, author=c1, counted_at=T6_R1); self.count(ln, 6, author=c2, counted_at=T6_R2)  # case 14
        self.reject(ln, resolved_at=T6_RESOLVED, origin=ResolutionOrigin.AUDIT)
        # 2 CLOSED by the rules (case 7).
        for _ in range(2):
            ln = line(); self.count(ln, K(ln), author=c1, counted_at=T6_R1)  # case 7

        t6_lines = [line_ for line_ in self.lines if line_.task.id == t6._id]
        assert len(t6_lines) == 5 and all(not line_.is_open for line_ in t6_lines)
        self.event(
            Events.TASK_RELEASED, t6, self.planner, AuditActivity.MODIFICATION, T6_RELEASED,
            context={"total_quantity": 5, "omitted_quantity": 0, "is_zero_stock_included": False, "items_without_stock": []},
        )
        for author, at, round_ in ((c1, T6_R1, 1), (c2, T6_R2, 2)):
            quantity = sum(1 for line_ in t6_lines for c in line_.counts if c.counted_at == at and c.author.id == author.id)
            self.captured(t6, author, at, quantity, round_)
        self.event(
            Events.TASK_CLOSED, t6, self.auditor, AuditActivity.MODIFICATION, T6_CLOSED,
            context={"resolved_quantity": 5, "adjusted_quantity": 2, "rejected_quantity": 1},
        )

    # ---- persistence

    def write(self) -> Dict[str, Tuple[int, int]]:
        """Insert everything that is missing. Returns `{collection: (inserted, already there)}`."""
        summary: Dict[str, Tuple[int, int]] = {}
        inserted = existing = 0
        for task in self.tasks:
            if task._id in self._existing_tasks:
                existing += 1
                continue
            self.tasks_repo.set(task._id, data=to_doc(task), write_only_if_insert=True)
            inserted += 1
        summary[TASKS_ENTITY] = (inserted, existing)

        inserted = existing = 0
        for schedule in self.schedules:
            matched = self.schedules_repo.set(schedule._id, data=to_doc(schedule), write_only_if_insert=True)
            inserted += 0 if matched else 1
            existing += 1 if matched else 0
        summary[InventoryCountScheduleModel.get_entity_name()] = (inserted, existing)

        inserted, duplicated, locked = self.lines_repo.insert_many_ignore_duplicates(
            [to_doc(line) for line in self.lines]
        )
        if locked:
            print(
                f"WARNING: {locked} lines collided with the D-02 lock: their location contents are "
                "open in another task of the same count type (not created by this seed)",
                file=sys.stderr,
            )
        summary[InventoryCountLineModel.get_entity_name()] = (inserted, duplicated + locked)

        inserted = existing = 0
        for event in self.events:
            matched = self.events_repo.set(event._id, data=to_doc(event), write_only_if_insert=True)
            inserted += 0 if matched else 1
            existing += 1 if matched else 0
        summary[InventoryCountEventModel.get_entity_name()] = (inserted, existing)
        return summary

    def verify(self) -> None:
        """Every seeded document must come back through `Model(**doc)`."""
        task_ids = [task._id for task in self.tasks]
        db = self.manager.get_database()
        checks = [
            (InventoryCountTaskModel, {"_id": {"$in": task_ids}}),
            (InventoryCountLineModel, {"task.id": {"$in": task_ids}}),
            (InventoryCountEventModel, {"$or": [{"task.id": {"$in": task_ids}}, {"schedule.id": {"$in": [s._id for s in self.schedules]}}]}),
            (InventoryCountScheduleModel, {"_id": {"$in": [s._id for s in self.schedules]}}),
        ]
        for model, filter_ in checks:
            docs = list(db[model.get_entity_name()].find(filter_))
            for doc in docs:
                model(**doc)
            print(f"  {model.get_entity_name()}: {len(docs)} documents rebuilt with {model.__name__}(**doc)")
        open_lines = [line for line in self.lines if line.is_open and line.deleted_at is None]
        keys = {(line.task.count_type, line.location_content_id) for line in open_lines}
        assert len(keys) == len(open_lines), "D-02: a location content is open twice in the same count type"
        for task in self.tasks:
            lines = task.lines
            assert lines.resolved_quantity <= lines.counted_quantity <= lines.total_quantity, task.task_name
        print("  D-02 lock and task counters are consistent")


def reset(manager, umu_id: str) -> None:
    """Delete only what the seed created: tasks/schedules named with the prefix, their lines and events."""
    db = manager.get_database()
    name_filter = {"umu_id": umu_id, "task_name": {"$regex": r"^\[SEED\]"}}
    task_ids = [doc["_id"] for doc in db[TASKS_ENTITY].find(name_filter, projection={"_id": 1})]
    schedule_ids = [
        doc["_id"]
        for doc in db[InventoryCountScheduleModel.get_entity_name()].find(
            {"umu_id": umu_id, "schedule_name": {"$regex": r"^\[SEED\]"}}, projection={"_id": 1}
        )
    ]
    lines = db[InventoryCountLineModel.get_entity_name()].delete_many({"task.id": {"$in": task_ids}}).deleted_count
    events = db[InventoryCountEventModel.get_entity_name()].delete_many(
        {"umu_id": umu_id, "$or": [{"task.id": {"$in": task_ids}}, {"schedule.id": {"$in": schedule_ids}}]}
    ).deleted_count
    tasks = db[TASKS_ENTITY].delete_many({"_id": {"$in": task_ids}}).deleted_count
    schedules = db[InventoryCountScheduleModel.get_entity_name()].delete_many({"_id": {"$in": schedule_ids}}).deleted_count
    print(f"reset {umu_id}: {tasks} tasks, {lines} lines, {events} events, {schedules} schedules deleted "
          "(reference-counters untouched: gaps are accepted, D-09)")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--umu", default=DEFAULT_UMU, help=f"UMU to seed (default {DEFAULT_UMU})")
    parser.add_argument("--db", help="database name (overrides MONGODB_DB / MONGO_DB_NAME)")
    parser.add_argument("--reset", action="store_true", help="delete the [SEED] documents of the UMU first")
    parser.add_argument("--no-seed", action="store_true", help="with --reset: only delete")
    args = parser.parse_args()

    manager = connect(args.db)
    print(f"database: {database_name(manager)}")
    if args.reset:
        reset(manager, args.umu)
        if args.no_seed:
            return 0

    ensured = create_inventory_count_indexes(manager.get_database())
    print("regular indexes ensured:", ", ".join(f"{col} ({len(names)})" for col, names in ensured.items()))

    seed = Seed(manager, args.umu)
    seed.load_inventory()
    seed.build()
    summary = seed.write()
    print("seeded documents (inserted, already there):")
    for collection, (inserted, existing) in summary.items():
        print(f"  {collection}: {inserted} inserted, {existing} already there")
    for task in seed.tasks:
        print(f"  {task.reference_id} {task.task_name} [{task.status.value}] lines={asdict(task.lines)}")
    print("verifying:")
    seed.verify()
    return 0


if __name__ == "__main__":
    sys.exit(main())
