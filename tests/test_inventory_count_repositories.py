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

NOT_DELETED = {"$ne": True}


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
    assert repo.increment("t1", umu_id="u1", counters={"lines.counted_quantity": 3}) == 1
    collection.update_one.assert_called_once_with(
        {"_id": "t1", "umu_id": "u1"},
        {"$inc": {"lines.counted_quantity": 3}},
        upsert=False,
    )


def test_soft_delete_marks_without_deleting(db_manager, collection):
    repo = InventoryCountLinesRepository(db_manager, "inventory-count-lines")
    collection.update_many.return_value.modified_count = 5
    assert repo.soft_delete_by_task("t1", umu_id="u1") == 5
    collection.delete_many.assert_not_called()
    filter, update = collection.update_many.call_args.args
    assert filter == {"umu_id": "u1", "task.id": "t1", "is_deleted": NOT_DELETED}
    assert update["$set"]["is_deleted"] is True
    assert update["$set"]["is_open"] is False
    assert isinstance(update["$set"]["deleted_at"], int)


def test_line_reads_exclude_soft_deleted(db_manager, collection):
    repo = InventoryCountLinesRepository(db_manager, "inventory-count-lines")
    repo.get("l1", umu_id="u1")
    assert collection.find_one.call_args.args[0] == {
        "_id": "l1",
        "is_deleted": NOT_DELETED,
        "umu_id": "u1",
    }
    collection.count_documents.return_value = 0
    collection.find.return_value = []
    repo.get_paginated(
        2, 20, umu_id="u1", and_conditions=[("status", "in", ["PENDING"])]
    )
    expected = {"status": {"$in": ["PENDING"]}, "is_deleted": NOT_DELETED, "umu_id": "u1"}
    collection.count_documents.assert_called_once_with(expected)
    assert collection.find.call_args.args[0] == expected
    assert collection.find.call_args.kwargs["skip"] == 20


def test_purge_deletes_only_soft_deleted_in_batch(db_manager, collection):
    repo = InventoryCountLinesRepository(db_manager, "inventory-count-lines")
    collection.find.return_value = [{"_id": "a"}, {"_id": "b"}]
    collection.delete_many.return_value.deleted_count = 2
    assert repo.delete_many("t1", umu_id="u1", limit=1000) == 2
    assert collection.find.call_args.args[0] == {
        "umu_id": "u1",
        "task.id": "t1",
        "is_deleted": True,
    }
    assert collection.find.call_args.kwargs["limit"] == 1000
    collection.delete_many.assert_called_once_with({"_id": {"$in": ["a", "b"]}})


def test_purge_without_lines_does_not_delete(db_manager, collection):
    repo = InventoryCountLinesRepository(db_manager, "inventory-count-lines")
    collection.find.return_value = []
    assert repo.delete_many("t1", umu_id="u1") == 0
    collection.delete_many.assert_not_called()


def test_events_by_reference_applies_range_and_umu(db_manager, collection):
    repo = InventoryCountEventsRepository(db_manager, "inventory-count-events")
    collection.count_documents.return_value = 1
    collection.find.return_value = [{"_id": "e1"}]
    count, items = repo.get_by_reference_id(
        "CF-000001", umu_id="u1", created_at_gt=1, created_at_lt=9
    )
    expected = {
        "umu_id": "u1",
        "task.reference_id": "CF-000001",
        "created_at": {"$gte": 1, "$lte": 9},
    }
    collection.count_documents.assert_called_once_with(expected)
    assert collection.find.call_args.args[0] == expected
    assert count == 1 and list(items) == [{"_id": "e1"}]


def test_get_batch_after_keyset_without_skip(db_manager, collection):
    repo = LocationContentRepository(db_manager, "location-contents")
    collection.find.return_value = [{"_id": "x"}]
    assert repo.get_batch_after(
        "u1", "a", 500, label_code="FARB", quantity_gt=0, item_ids=["i1"]
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


def test_get_batch_after_optional_filters_omitted(db_manager, collection):
    repo = LocationContentRepository(db_manager, "location-contents")
    collection.find.return_value = []
    repo.get_batch_after("u1", None, 10)
    assert collection.find.call_args.args[0] == {"umu_id": "u1"}
