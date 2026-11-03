# Atlas Search indexes and DEV seed of the inventory count module (REP-07)

One JSON per index, in the full format accepted by the Atlas Administration API and the Atlas CLI (`name`, `database`, `collectionName`, `mappings`). The same file serves DEV and PROD: only `database` changes (the placeholder `<database>` is replaced by the scripts, or by hand in the UI).

| File | Collection | Index name (constant in the code) | Used by |
| --- | --- | --- | --- |
| `inventory-count-tasks.autocomplete_task_name_scheduled_at.json` | `inventory-count-tasks` | `TASK_NAME_SEARCH_INDEX` (`inventory_count_tasks.py`) | `search_by_task_name` |
| `inventory-count-schedules.autocomplete_schedule_name_created_at.json` | `inventory-count-schedules` | `SCHEDULE_NAME_SEARCH_INDEX` (`inventory_count_schedules.py`) | `search_by_schedule_name` |
| `inventory-count-lines.autocomplete_item_id_foreign_id_resolved_at.json` | `inventory-count-lines` | `ITEM_SEARCH_INDEX` (`inventory_count_lines.py`) | `search_by_item` |

`inventory-count-events` has no Atlas index: its search is an exact folio lookup with a mandatory date range, served by the regular indexes of `inventory_count_indexes.py`.

## Mapping rules

- `dynamic: false`: only the fields that the `search_by_*` methods use in `must`, `filter`, `mustNot` and `sort`. A field missing from the mapping does not raise: the filter just matches nothing (or the sort fails at run time).
- `autocomplete` with `edgeGram`, `foldDiacritics: true` and `lucene.standard` (lower-cases): "físico" and "fisico" match the same task. `task_name` / `schedule_name` use 2–20 grams; `item.id` / `item.foreign_id` copy the existing `location-contents` index (`autocomplete_item_id_range_expiration_date_range_quantity`, 2–15 grams) so the search by key behaves like the inventory screen.
- `token` without normalizer for the exact values (`umu_id`, `status`, enums, `lot`, `task.id`). Atlas resolves `equals` and `in` on `token`.
- Nested fields (`item`, `task`, `resolution`) are mapped as `document`.
- `deleted_at` is mapped as `number` and the soft deleted lines are excluded with a **numeric range** (`mustNot: [{range: {path: "deleted_at", gte: 0}}]`, helper `_search.has_number`), **not with `exists`**. Verified against Atlas DEV on 2026-11-03: a live line is stored as `deleted_at: null` (`.dict()` keeps every key) and the `exists` operator treats a null key as existing, so `mustNot: exists` returned 0 lines out of 60, while the range matched exactly the 5 deleted ones. The same applies to any optional field written as `null`.

**If a mapping changes, the index has to be dropped and created again**: Atlas does not apply mapping changes to an existing index (`--recreate` below).

## How to create them

### Option A (used in DEV): `scripts/create_atlas_search_indexes.py`

pymongo only, no Atlas CLI needed. Needs a database user with read/write on the database (the `createSearchIndexes` command, MongoDB 7.0+ on Atlas). It creates the collection when it does not exist yet (a search index needs the namespace) and waits until the three indexes are `READY`.

```bash
# from the repo root, in a virtualenv
pip install -r scripts/requirements-seed.txt -e .
# connection: MONGODB_URI + MONGODB_DB, or the MONGO_DB_* variables of a service .env (infra-mongodb-python)
python scripts/create_atlas_search_indexes.py            # create the missing ones, wait for READY
python scripts/create_atlas_search_indexes.py --status   # only print the status (exit 1 if any is not READY)
python scripts/create_atlas_search_indexes.py --recreate # drop + create (after a mapping change)
python scripts/create_atlas_search_indexes.py --db other # another database name
```

### Option B: Atlas CLI (role "Project Search Index Editor" on the Atlas project)

```bash
sed 's/<database>/<db>/' atlas_search_indexes/inventory-count-tasks.autocomplete_task_name_scheduled_at.json > /tmp/idx.json
atlas clusters search indexes create --clusterName <cluster> --projectId <project-id> --file /tmp/idx.json
atlas clusters search indexes list --clusterName <cluster> --db <db> --collection inventory-count-tasks
```

### Option C: Atlas UI

Search → Create Search Index → JSON Editor: paste only the `mappings` object and type the index name by hand (it must match the constant in the code).

## Where they are created

| Environment | Cluster | Database | Created on | Status |
| --- | --- | --- | --- | --- |
| DEV | `farmacias-prod.ah5rw.mongodb.net` (the cluster shared with PROD; the DEV database is `pharmagob`, PROD is `db-farmacias-prod`) | `pharmagob` | 2026-11-03, option A | `READY` (the three) |
| PROD | INF-07, same JSON with `database: db-farmacias-prod` | | | pending |

## DEV seed: `scripts/seed_inventory_count_dev.py`

Loads test data built **with the domain models** (never hand written) into `inventory-count-tasks`, `inventory-count-lines`, `inventory-count-events`, `inventory-count-schedules` and `reference-counters`, over real location contents, items and locations of one UMU. `location-contents` is never modified. See the module docstring for the full behaviour.

```bash
python scripts/seed_inventory_count_dev.py                     # default UMU MCIMB003562 (has general and FARB stock in DEV)
python scripts/seed_inventory_count_dev.py --umu MCIMB001812   # another UMU with STANDARD + FARB locations and stock >= 10
python scripts/seed_inventory_count_dev.py --reset             # delete the [SEED] documents of the UMU, then seed again
python scripts/seed_inventory_count_dev.py --reset --no-seed   # only delete
```

- Idempotent: task and schedule `_id`s are `uuid_by_params("SEED", umu, key)`, line and event ids are the domain's, every timestamp is a fixed constant. A second run inserts nothing (`0 inserted, N already there`).
- It ensures the regular indexes of REP-05 first (`create_inventory_count_indexes`): the D-02 lock and the unique `reference_id` must exist before the inserts.
- Folios come from `reference-counters` through `ReferenceCounterService` and are consumed only on the first insert of each task; `--reset` leaves the counters as they are (gaps are accepted, D-09).
- Every line's `status`, `rule.case` and `rule.note` come from `inventory_count_rules.evaluate`, round by round; APPLYING / ADJUSTED / REJECTED / returned to audit are applied on top the way the services do it, after asserting the engine's state.
- The `APPLYING` line is written with the **whole** `resolution` (type, origin, author, resolved_at, quantities) so that `InventoryCountLineModel(**doc)` works; the real claim of ADJ-03 (H-21) only writes `resolution.resolved_at`, which that model cannot read back (Technical Finding TF-010).
- **Test users**: `planeador.seed@cenadigs.com`, `contador.seed@…`, `contador2.seed@…`, `auditor.seed@…` (constants at the top of the script). Replace the ids with the Keycloak DEV `sub`/email of the users with roles `Planeador_de_Conteos`, `Contador_de_Piso` and `Auditor_de_Ajustes` when they exist, then `--reset` and seed again.
- Deviation from the guide: T2 (`[SEED] Cíclico controlados`) is `cyclic_mode: SCHEDULED` with `schedule` = S1, because S1 has `last_task = T2`; T4 keeps the `MANUAL` example. A manual cyclic task with no schedule and a scheduled one are both covered.

What it leaves for the default UMU (reference ids depend on the counter):

| Task | Type / mode | Scope | Status | Lines | Events |
| --- | --- | --- | --- | --- | --- |
| `[SEED] Físico cerrado septiembre` | PHYSICAL | ALL | CLOSED | 15, all resolved (11 CLOSED, 2 ADJUSTED, 2 REJECTED) | TASK_SCHEDULED, TASK_RELEASED, COUNT_CAPTURED ×4 (two authors), ADJUSTMENT_POSITIVE, ADJUSTMENT_NEGATIVE, REJECTED ×2, TASK_CLOSED |
| `[SEED] Inventario Físico Octubre` | PHYSICAL | GENERAL | IN_PROGRESS | 30: 6 PENDING, 5 SECOND_COUNT (cases 3, 3, 9, 10, 5), 4 VALIDATION (12, 14, returned INSUFFICIENT_STOCK, returned SENT_BY_AUDITOR), 4 ADJUSTMENT (2, 4, 10, 9), 1 APPLYING, 5 CLOSED (7 ×3, 13 ×2), 3 ADJUSTED (+, −, out of system), 2 REJECTED (AUDIT, ADJUSTMENTS) | TASK_SCHEDULED, TASK_RELEASED, COUNT_CAPTURED ×4, OUT_OF_SYSTEM_REGISTERED ×4, AUDIT_VALIDATED ×3 (two on the same line), ADJUSTMENT_RETURNED_TO_AUDIT ×3, ADJUSTMENT_POSITIVE ×2, ADJUSTMENT_NEGATIVE, REJECTED ×2 |
| `[SEED] Cancelada sin capturas` | CYCLIC / MANUAL | GENERAL | CANCELLED | 5 with `deleted_at` numeric and `is_open: false` | TASK_SCHEDULED, TASK_RELEASED, TASK_CANCELLED |
| `[SEED] Cíclico controlados` | CYCLIC / SCHEDULED | BIENESTAR | OPEN | 10 PENDING on the FARB location, `item_filter.item_ids` with 2 keys without stock | TASK_SCHEDULED, TASK_RELEASED (`context.items_without_stock`) |
| `[SEED] Programado noviembre` | PHYSICAL | GENERAL | SCHEDULED | 0 | TASK_SCHEDULED |
| `[SEED] Semanal bienestar` (schedule) | WEEKLY, ACTIVE, `next_run_at` 2026-11-10, `last_task` = Cíclico controlados | | | | SCHEDULE_CREATED |

## Validation: `scripts/validate_atlas_searches.py`

Runs the 12 checks of the REP-07 guide (section 2.3) through the repository methods, so the validated pipeline is the one the services run. It waits (default 180 s) until Atlas has indexed the seed.

```bash
python scripts/validate_atlas_searches.py --umu MCIMB003562
```

Result in DEV on 2026-11-03: `12/12 checks passed` (output in the PR of REP-07).
