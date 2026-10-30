import pytest

from pharmagob.mongodb_repositories.base import BaseMongoDbRepository
from pharmagob.mongodb_repositories.inventory_count_events import (
    InventoryCountEventsRepository,
)
from pharmagob.mongodb_repositories.inventory_count_lines import (
    InventoryCountLinesRepository,
)
from pharmagob.mongodb_repositories.inventory_count_schedules import (
    InventoryCountSchedulesRepository,
)
from pharmagob.mongodb_repositories.inventory_count_tasks import (
    InventoryCountTasksRepository,
)
from pharmagob.mongodb_repositories.location_contents import LocationContentRepository

@pytest.mark.parametrize(
    "repo_cls,name",
    [
        (InventoryCountTasksRepository, "inventory-count-tasks"),
        (InventoryCountSchedulesRepository, "inventory-count-schedules"),
        (InventoryCountLinesRepository, "inventory-count-lines"),
        (InventoryCountEventsRepository, "inventory-count-events"),
    ],
)
def test_repositories_bind_collection(repo_cls, name, db_manager):
    repo = repo_cls(db_manager, name)
    assert isinstance(repo, BaseMongoDbRepository)
    db_manager.get_collection.assert_called_once_with(name)


def test_task_increment_uses_inc_and_umu(db_manager, collection):
    repo = InventoryCountTasksRepository(db_manager, "inventory-count-tasks")
    collection.update_one.return_value.modified_count = 1
    assert (
        repo.increment("t1", quantities={"lines.counted_quantity": 3}, umu_id="u1") == 1
    )
    collection.update_one.assert_called_once_with(
        {"_id": "t1", "umu_id": "u1"},
        {"$inc": {"lines.counted_quantity": 3}},
        upsert=False,
    )


def test_task_increment_umu_is_optional(db_manager, collection):
    repo = InventoryCountTasksRepository(db_manager, "inventory-count-tasks")
    repo.increment("t1", quantities={"lines.total_quantity": 1})
    assert collection.update_one.call_args.args[0] == {"_id": "t1"}


def test_soft_delete_marks_without_deleting(db_manager, collection):
    repo = InventoryCountLinesRepository(db_manager, "inventory-count-lines")
    collection.update_many.return_value.modified_count = 5
    assert repo.soft_delete_by_task("t1", deleted_at=1234, umu_id="u1") == 5
    collection.delete_many.assert_not_called()
    filter, update = collection.update_many.call_args.args
    assert filter == {"task.id": "t1", "deleted_at": None, "umu_id": "u1"}
    assert update == {"$set": {"deleted_at": 1234, "is_open": False}}
    assert "is_deleted" not in update["$set"]


def test_line_reads_exclude_soft_deleted(db_manager, collection):
    repo = InventoryCountLinesRepository(db_manager, "inventory-count-lines")
    repo.get("l1", umu_id="u1")
    assert collection.find_one.call_args.args[0] == {
        "_id": "l1",
        "deleted_at": None,
        "umu_id": "u1",
    }
    collection.count_documents.return_value = 0
    collection.find.return_value = []
    repo.get_paginated(
        2, 20, umu_id="u1", and_conditions=[("status", "in", ["PENDING"])]
    )
    expected = {"status": {"$in": ["PENDING"]}, "deleted_at": None, "umu_id": "u1"}
    collection.count_documents.assert_called_once_with(expected)
    assert collection.find.call_args.args[0] == expected
    assert collection.find.call_args.kwargs["skip"] == 20


def test_purge_deletes_the_conditions_in_batch(db_manager, collection):
    repo = InventoryCountLinesRepository(db_manager, "inventory-count-lines")
    collection.find.return_value = [{"_id": "a"}, {"_id": "b"}]
    collection.delete_many.return_value.deleted_count = 2
    conditions = [
        ("task.id", "=", "t1"),
        ("umu_id", "=", "u1"),
        ("deleted_at", "not in", [None]),
    ]
    assert repo.delete_many(conditions, limit=1000) == 2
    assert collection.find.call_args.args[0] == {
        "task.id": {"$eq": "t1"},
        "umu_id": {"$eq": "u1"},
        "deleted_at": {"$nin": [None]},
    }
    assert collection.find.call_args.kwargs["limit"] == 1000
    collection.delete_many.assert_called_once_with({"_id": {"$in": ["a", "b"]}})


def test_purge_default_limit_is_the_batch_size(db_manager, collection):
    repo = InventoryCountLinesRepository(db_manager, "inventory-count-lines")
    collection.find.return_value = []
    assert repo.delete_many([("deleted_at", "not in", [None])]) == 0
    assert collection.find.call_args.kwargs["limit"] == 1000
    collection.delete_many.assert_not_called()


def test_purge_requires_conditions(db_manager, collection):
    repo = InventoryCountLinesRepository(db_manager, "inventory-count-lines")
    with pytest.raises(ValueError):
        repo.delete_many(None)
    with pytest.raises(ValueError):
        repo.delete_many([])
    collection.delete_many.assert_not_called()


def test_events_by_reference_exclusive_range_and_filters(db_manager, collection):
    repo = InventoryCountEventsRepository(db_manager, "inventory-count-events")
    collection.count_documents.return_value = 1
    collection.find.return_value = [{"_id": "e1"}]
    count, items = repo.search_by_reference_id(
        "CF-000001",
        created_at_gt=1,
        created_at_lt=9,
        umu_id="u1",
        event=["AUDIT_VALIDATED", "REJECTED"],
        author_id="a1",
        page=3,
        limit=20,
    )
    expected = {
        "task.reference_id": "CF-000001",
        "created_at": {"$gt": 1, "$lt": 9},
        "umu_id": "u1",
        "event": {"$in": ["AUDIT_VALIDATED", "REJECTED"]},
        "author.id": "a1",
    }
    collection.count_documents.assert_called_once_with(expected)
    args, kwargs = collection.find.call_args
    assert args[0] == expected
    assert kwargs["skip"] == 40 and kwargs["limit"] == 20
    assert kwargs["sort"] == [("created_at", -1)]
    assert count == 1 and items == [{"_id": "e1"}]


def test_events_by_reference_optional_filters_omitted(db_manager, collection):
    repo = InventoryCountEventsRepository(db_manager, "inventory-count-events")
    collection.count_documents.return_value = 0
    collection.find.return_value = []
    repo.search_by_reference_id("CF-000001", created_at_gt=1, created_at_lt=9)
    assert collection.find.call_args.args[0] == {
        "task.reference_id": "CF-000001",
        "created_at": {"$gt": 1, "$lt": 9},
    }
    assert collection.find.call_args.kwargs["skip"] == 0


def test_get_batch_after_keyset_without_skip(db_manager, collection):
    repo = LocationContentRepository(db_manager, "location-contents")
    collection.find.return_value = [{"_id": "x"}]
    assert repo.get_batch_after(
        "u1",
        after_id="a",
        limit=500,
        location_label_code="FARB",
        quantity_gt=0,
        item_ids=["i1"],
    ) == [{"_id": "x"}]
    args, kwargs = collection.find.call_args
    assert args[0] == {
        "umu_id": "u1",
        "_id": {"$gt": "a"},
        "location.label_code": "FARB",
        "quantity": {"$gt": 0},
        "item.id": {"$in": ["i1"]},
    }
    assert kwargs["sort"] == [("_id", 1)]
    assert kwargs["limit"] == 500
    assert "skip" not in kwargs


def test_get_batch_after_general_scope_uses_not_in(db_manager, collection):
    repo = LocationContentRepository(db_manager, "location-contents")
    collection.find.return_value = []
    repo.get_batch_after("u1", location_label_code_not_in=["FARB"])
    assert collection.find.call_args.args[0] == {
        "umu_id": "u1",
        "location.label_code": {"$nin": ["FARB"]},
    }


def test_get_batch_after_defaults_and_optional_filters_omitted(db_manager, collection):
    repo = LocationContentRepository(db_manager, "location-contents")
    collection.find.return_value = []
    repo.get_batch_after("u1")
    assert collection.find.call_args.args[0] == {"umu_id": "u1"}
    assert collection.find.call_args.kwargs["limit"] == 1000
