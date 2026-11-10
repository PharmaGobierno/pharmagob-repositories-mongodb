# pharmagob-repositories-mongodb

## Inventory count repositories

Mongo adapters of the DOM-04 ports (`pharmagob-domain-python`):

| Repository | Highlights |
|---|---|
| `InventoryCountTasksRepository` | `increment`, `claim`, `search_by_task_name` (Atlas), `search_by_reference_id` (exact folio) |
| `InventoryCountSchedulesRepository` | `claim`, `search_by_schedule_name` (Atlas) |
| `InventoryCountLinesRepository` | `claim`, `insert_many_ignore_duplicates`, soft delete by `deleted_at` + `delete_many` purge, `search_by_item` (Atlas) |
| `InventoryCountEventsRepository` | `search_by_reference_id` (exact folio, mandatory exclusive date range) |
| `ReferenceCounterRepository` | `next_sequence` on the shared `reference-counters` collection |
| `LocationContentRepository` | `get_batch_after` (keyset sweep by `_id`) |

Lines are soft deleted with `deleted_at` (never `is_deleted`: the domain model has no such field). Every read of the lines repository leaves them out.

### Indexes

Regular indexes (epic 5.11) are created per environment, idempotently:

```
MONGODB_URI=... MONGODB_DB=... python -m pharmagob.mongodb_repositories.inventory_count_indexes
```

The `search_by_*` methods that use Atlas Search need these search indexes, versioned apart as JSON (they are not created from code):

| Index | Collection | Fields |
|---|---|---|
| `autocomplete_task_name_scheduled_at` | `inventory-count-tasks` | `task_name` (autocomplete), `umu_id`, `count_type`, `status`, `cyclic_mode`, `scheduled_at` |
| `autocomplete_schedule_name_created_at` | `inventory-count-schedules` | `schedule_name` (autocomplete), `umu_id`, `status`, `frequency`, `created_at` |
| `autocomplete_item_id_foreign_id_resolved_at` | `inventory-count-lines` | `item.id`, `item.foreign_id` (autocomplete), `umu_id`, `task.id`, `status`, `lot`, `is_in_system`, `resolution.*`, `pending_adjustment_quantity` (sign filter of Adjustments, H-28), `deleted_at`, `created_at` |

The definitions live in [`atlas_search_indexes/`](atlas_search_indexes/README.md) (one JSON per index, full Atlas API format) together with the scripts that create them, seed DEV with documents built from the domain models and run the 12 search checks (`scripts/create_atlas_search_indexes.py`, `scripts/seed_inventory_count_dev.py`, `scripts/validate_atlas_searches.py`). Soft deleted lines are excluded from `search_by_item` with a numeric range on `deleted_at` (`_search.has_number`), never with `exists`: Atlas treats `deleted_at: null` as existing.
