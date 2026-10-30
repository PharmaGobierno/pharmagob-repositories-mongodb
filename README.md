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
| `autocomplete_item_id_foreign_id_resolved_at` | `inventory-count-lines` | `item.id`, `item.foreign_id` (autocomplete), `umu_id`, `task.id`, `status`, `lot`, `is_in_system`, `resolution.*`, `deleted_at`, `created_at` |
