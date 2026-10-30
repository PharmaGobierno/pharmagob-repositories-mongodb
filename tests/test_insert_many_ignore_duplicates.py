import mongomock
import pytest
from pymongo.errors import BulkWriteError

from pharmagob.mongodb_repositories.inventory_count_lines import (
    InventoryCountLinesRepository,
)


def line(_id, location_content_id, *, count_type="PHYSICAL", is_open=True):
    return {
        "_id": _id,
        "umu_id": "u1",
        "task": {"id": "t1", "count_type": count_type},
        "location_content_id": location_content_id,
        "is_open": is_open,
    }


@pytest.fixture
def real_collection():
    collection = mongomock.MongoClient().db.lines
    collection.create_index(
        [("umu_id", 1), ("task.count_type", 1), ("location_content_id", 1)],
        unique=True,
        partialFilterExpression={"is_open": True},
        name="count_type_lock",
    )
    return collection


@pytest.fixture
def repo(mocker, real_collection):
    manager = mocker.MagicMock()
    manager.get_collection.return_value = real_collection
    return InventoryCountLinesRepository(manager, "inventory-count-lines")


def test_all_new_lines_are_inserted(repo, real_collection):
    assert repo.insert_many_ignore_duplicates([line("a", "L1"), line("b", "L2")]) == (
        2,
        0,
        0,
    )
    assert real_collection.count_documents({}) == 2


def test_empty_input_does_not_call_insert_many(db_manager, collection):
    repo = InventoryCountLinesRepository(db_manager, "inventory-count-lines")
    assert repo.insert_many_ignore_duplicates([]) == (0, 0, 0)
    collection.insert_many.assert_not_called()


def test_redelivery_counts_as_duplicated(repo, real_collection):
    real_collection.insert_one(line("a", "L1"))
    assert repo.insert_many_ignore_duplicates([line("a", "L1"), line("b", "L2")]) == (
        1,
        1,
        0,
    )
    assert sorted(d["_id"] for d in real_collection.find()) == ["a", "b"]


def test_repeated_key_in_the_same_batch_is_duplicated(repo):
    assert repo.insert_many_ignore_duplicates([line("a", "L1"), line("a", "L2")]) == (
        1,
        1,
        0,
    )


def test_open_in_another_task_of_the_same_type_is_locked(repo, real_collection):
    real_collection.insert_one(line("old", "L1"))
    assert repo.insert_many_ignore_duplicates([line("new", "L1"), line("ok", "L2")]) == (
        1,
        0,
        1,
    )
    assert sorted(d["_id"] for d in real_collection.find()) == ["ok", "old"]


def test_other_count_type_or_closed_line_is_not_locked(repo, real_collection):
    real_collection.insert_one(line("old", "L1"))
    inserted = repo.insert_many_ignore_duplicates(
        [line("cyclic", "L1", count_type="CYCLIC"), line("closed", "L1", is_open=False)]
    )
    assert inserted == (2, 0, 0)


def test_duplicated_and_locked_in_the_same_batch(repo, real_collection):
    real_collection.insert_one(line("a", "L1"))
    batch = [line("a", "L1"), line("b", "L1"), line("c", "L3")]
    assert repo.insert_many_ignore_duplicates(batch) == (1, 1, 1)


def test_classifies_by_key_pattern_when_the_server_reports_it(db_manager, collection):
    repo = InventoryCountLinesRepository(db_manager, "inventory-count-lines")
    collection.insert_many.side_effect = BulkWriteError(
        {
            "nInserted": 1,
            "writeErrors": [
                {"index": 0, "code": 11000, "keyPattern": {"_id": 1}},
                {
                    "index": 1,
                    "code": 11000,
                    "keyPattern": {
                        "umu_id": 1,
                        "task.count_type": 1,
                        "location_content_id": 1,
                    },
                },
            ],
        }
    )
    assert repo.insert_many_ignore_duplicates([line("a", "L1"), line("b", "L1")]) == (
        1,
        1,
        1,
    )
    collection.count_documents.assert_not_called()


def test_other_write_errors_are_raised(db_manager, collection):
    repo = InventoryCountLinesRepository(db_manager, "inventory-count-lines")
    collection.insert_many.side_effect = BulkWriteError(
        {"nInserted": 0, "writeErrors": [{"index": 0, "code": 121, "errmsg": "invalid"}]}
    )
    with pytest.raises(BulkWriteError):
        repo.insert_many_ignore_duplicates([line("a", "L1")])
