import mongomock
import pytest

from pharmagob.mongodb_repositories.inventory_count_lines import (
    ITEM_SEARCH_INDEX,
    InventoryCountLinesRepository,
)
from pharmagob.mongodb_repositories.inventory_count_schedules import (
    SCHEDULE_NAME_SEARCH_INDEX,
    InventoryCountSchedulesRepository,
)
from pharmagob.mongodb_repositories.inventory_count_tasks import (
    TASK_NAME_SEARCH_INDEX,
    InventoryCountTasksRepository,
)


def stub_aggregate(collection, results, count):
    collection.aggregate.return_value.next.return_value = {
        "results": results,
        "count": count,
    }


def pipeline_of(collection):
    return collection.aggregate.call_args.kwargs["pipeline"]


def test_task_name_search_is_atlas_autocomplete_with_filters(db_manager, collection):
    repo = InventoryCountTasksRepository(db_manager, "inventory-count-tasks")
    stub_aggregate(collection, [{"_id": "t1"}], 41)
    count, items = repo.search_by_task_name(
        "cíclico",
        page=3,
        limit=20,
        umu_id="u1",
        count_type="CYCLIC",
        status=["OPEN", "IN_PROGRESS"],
        cyclic_mode="MANUAL",
        scheduled_at_gt=5,
        scheduled_at_lt=9,
    )
    assert (count, items) == (41, [{"_id": "t1"}])
    search, facet, _ = pipeline_of(collection)
    assert search["$search"]["index"] == TASK_NAME_SEARCH_INDEX
    compound = search["$search"]["compound"]
    assert compound["must"] == [
        {"autocomplete": {"query": "cíclico", "path": "task_name"}}
    ]
    assert compound["filter"] == [
        {"equals": {"path": "umu_id", "value": "u1"}},
        {"equals": {"path": "count_type", "value": "CYCLIC"}},
        {"in": {"path": "status", "value": ["OPEN", "IN_PROGRESS"]}},
        {"equals": {"path": "cyclic_mode", "value": "MANUAL"}},
        {"range": {"path": "scheduled_at", "gt": 5, "lt": 9}},
    ]
    assert search["$search"]["sort"] == {"scheduled_at": -1}
    assert facet["$facet"]["results"] == [{"$skip": 40}, {"$limit": 20}]


def test_search_without_results_returns_zero(db_manager, collection):
    repo = InventoryCountTasksRepository(db_manager, "inventory-count-tasks")
    collection.aggregate.return_value.next.return_value = {"results": []}
    assert repo.search_by_task_name("nada") == (0, [])


def test_schedule_name_search(db_manager, collection):
    repo = InventoryCountSchedulesRepository(db_manager, "inventory-count-schedules")
    stub_aggregate(collection, [], 0)
    repo.search_by_schedule_name(
        "semanal", umu_id="u1", status="ACTIVE", frequency="WEEKLY"
    )
    search = pipeline_of(collection)[0]["$search"]
    assert search["index"] == SCHEDULE_NAME_SEARCH_INDEX
    assert search["compound"]["must"] == [
        {"autocomplete": {"query": "semanal", "path": "schedule_name"}}
    ]
    assert search["compound"]["filter"] == [
        {"equals": {"path": "umu_id", "value": "u1"}},
        {"equals": {"path": "status", "value": "ACTIVE"}},
        {"equals": {"path": "frequency", "value": "WEEKLY"}},
    ]


def test_line_item_search_matches_id_or_foreign_id_and_hides_deleted(
    db_manager, collection
):
    repo = InventoryCountLinesRepository(db_manager, "inventory-count-lines")
    stub_aggregate(collection, [{"_id": "l1"}], 1)
    repo.search_by_item(
        "010.000",
        umu_id="u1",
        task_id="t1",
        status=["ADJUSTMENT"],
        lot="L1",
        is_in_system=False,
        resolution_type="ADJUSTED",
        resolution_origin="AUDIT",
        resolved_at_gt=1,
        resolved_at_lt=2,
    )
    search = pipeline_of(collection)[0]["$search"]
    assert search["index"] == ITEM_SEARCH_INDEX
    compound = search["compound"]
    assert compound["must"] == [
        {
            "compound": {
                "should": [
                    {"autocomplete": {"query": "010.000", "path": "item.id"}},
                    {"autocomplete": {"query": "010.000", "path": "item.foreign_id"}},
                ],
                "minimumShouldMatch": 1,
            }
        }
    ]
    assert compound["filter"] == [
        {"equals": {"path": "umu_id", "value": "u1"}},
        {"equals": {"path": "task.id", "value": "t1"}},
        {"in": {"path": "status", "value": ["ADJUSTMENT"]}},
        {"equals": {"path": "lot", "value": "L1"}},
        {"equals": {"path": "is_in_system", "value": False}},
        {"equals": {"path": "resolution.type", "value": "ADJUSTED"}},
        {"equals": {"path": "resolution.origin", "value": "AUDIT"}},
        {"range": {"path": "resolution.resolved_at", "gt": 1, "lt": 2}},
    ]
    assert compound["mustNot"] == [{"exists": {"path": "deleted_at"}}]


@pytest.mark.parametrize(
    "sign,bound", [("POSITIVE", {"gt": 0}), ("NEGATIVE", {"lt": 0})]
)
def test_line_adjustment_sign_reads_the_adjustment_quantity(
    sign, bound, db_manager, collection
):
    repo = InventoryCountLinesRepository(db_manager, "inventory-count-lines")
    stub_aggregate(collection, [], 0)
    repo.search_by_item("x", adjustment_sign=sign)
    assert pipeline_of(collection)[0]["$search"]["compound"]["filter"] == [
        {"range": {"path": "resolution.adjustment_quantity", **bound}}
    ]


def test_line_unknown_adjustment_sign_is_rejected(db_manager, collection):
    repo = InventoryCountLinesRepository(db_manager, "inventory-count-lines")
    with pytest.raises(ValueError):
        repo.search_by_item("x", adjustment_sign="ZERO")
    collection.aggregate.assert_not_called()


@pytest.fixture
def real_tasks(mocker):
    collection = mongomock.MongoClient().db.tasks
    base = {"status": "OPEN", "count_type": "CYCLIC", "scheduled_at": 10}
    collection.insert_many(
        [
            {"_id": "1", "reference_id": "CF-000001", "umu_id": "u1", **base},
            {"_id": "2", "reference_id": "CF-000001", "umu_id": "u2", **base},
            {"_id": "3", "reference_id": "CF-000002", "umu_id": "u1", **base},
        ]
    )
    manager = mocker.MagicMock()
    manager.get_collection.return_value = collection
    return InventoryCountTasksRepository(manager, "inventory-count-tasks")


def test_task_reference_search_is_exact_and_umu_scoped(real_tasks):
    count, items = real_tasks.search_by_reference_id("CF-000001", umu_id="u1")
    assert count == 1 and [i["_id"] for i in items] == ["1"]
    assert real_tasks.search_by_reference_id("CF-0000", umu_id="u1") == (0, [])


def test_task_reference_search_filters_and_paginates(real_tasks):
    assert real_tasks.search_by_reference_id(
        "CF-000001", status=["CLOSED"], scheduled_at_gt=1, scheduled_at_lt=99
    ) == (0, [])
    count, items = real_tasks.search_by_reference_id(
        "CF-000001",
        status=["OPEN"],
        scheduled_at_gt=9,
        scheduled_at_lt=11,
        page=2,
        limit=1,
    )
    assert count == 2 and len(items) == 1
