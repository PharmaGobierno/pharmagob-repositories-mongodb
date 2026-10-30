import threading
from enum import Enum

import mongomock
import pytest

from pharmagob.mongodb_repositories.inventory_count_lines import (
    InventoryCountLinesRepository,
)
from pharmagob.mongodb_repositories.inventory_count_schedules import (
    InventoryCountSchedulesRepository,
)
from pharmagob.mongodb_repositories.inventory_count_tasks import (
    InventoryCountTasksRepository,
)

REPOSITORIES = [
    (InventoryCountTasksRepository, "inventory-count-tasks"),
    (InventoryCountSchedulesRepository, "inventory-count-schedules"),
    (InventoryCountLinesRepository, "inventory-count-lines"),
]


class Status(str, Enum):
    SCHEDULED = "SCHEDULED"
    RELEASING = "RELEASING"


@pytest.fixture
def real_collection():
    return mongomock.MongoClient().db.collection


@pytest.fixture
def real_db_manager(mocker, real_collection):
    manager = mocker.MagicMock()
    manager.get_collection.return_value = real_collection
    return manager


@pytest.mark.parametrize("repo_cls,name", REPOSITORIES)
def test_claim_filters_by_status_and_umu(repo_cls, name, db_manager, collection):
    repo = repo_cls(db_manager, name)
    collection.find_one_and_update.return_value = {"_id": "x", "status": "RELEASING"}
    claimed = repo.claim(
        "x",
        expected_statuses=[Status.SCHEDULED],
        data={"status": "RELEASING"},
        umu_id="u1",
    )
    assert claimed == {"_id": "x", "status": "RELEASING"}
    args, kwargs = collection.find_one_and_update.call_args
    assert args[0]["_id"] == "x"
    assert args[0]["status"] == {"$in": ["SCHEDULED"]}
    assert args[0]["umu_id"] == "u1"
    assert args[1] == {"$set": {"status": "RELEASING"}}
    assert kwargs["upsert"] is False
    assert kwargs["return_document"] is True  # ReturnDocument.AFTER


@pytest.mark.parametrize("repo_cls,name", REPOSITORIES)
def test_claim_returns_none_when_nobody_matches(
    repo_cls, name, db_manager, collection
):
    repo = repo_cls(db_manager, name)
    collection.find_one_and_update.return_value = None
    assert repo.claim("x", expected_statuses=["SCHEDULED"], data={}) is None


def test_claim_of_lines_ignores_soft_deleted(db_manager, collection):
    repo = InventoryCountLinesRepository(db_manager, "inventory-count-lines")
    repo.claim("l1", expected_statuses=["ADJUSTMENT"], data={"status": "APPLYING"})
    assert collection.find_one_and_update.call_args.args[0]["deleted_at"] is None


def test_claim_moves_the_document_once(real_db_manager, real_collection):
    real_collection.insert_one({"_id": "t1", "umu_id": "u1", "status": "SCHEDULED"})
    repo = InventoryCountTasksRepository(real_db_manager, "inventory-count-tasks")
    first = repo.claim(
        "t1",
        expected_statuses=["SCHEDULED"],
        data={"status": "RELEASING"},
        umu_id="u1",
    )
    second = repo.claim(
        "t1",
        expected_statuses=["SCHEDULED"],
        data={"status": "RELEASING"},
        umu_id="u1",
    )
    assert first["status"] == "RELEASING"
    assert second is None


def test_claim_other_umu_does_not_match(real_db_manager, real_collection):
    real_collection.insert_one({"_id": "t1", "umu_id": "u1", "status": "SCHEDULED"})
    repo = InventoryCountTasksRepository(real_db_manager, "inventory-count-tasks")
    assert (
        repo.claim(
            "t1",
            expected_statuses=["SCHEDULED"],
            data={"status": "RELEASING"},
            umu_id="u2",
        )
        is None
    )
    assert real_collection.find_one({"_id": "t1"})["status"] == "SCHEDULED"


def test_two_threads_claim_only_one_wins(real_db_manager, real_collection):
    """REP-02 acceptance: two threads race for the same task, one wins."""
    real_collection.insert_one({"_id": "t1", "umu_id": "u1", "status": "SCHEDULED"})
    repo = InventoryCountTasksRepository(real_db_manager, "inventory-count-tasks")
    barrier = threading.Barrier(2)
    results = []

    def worker(name):
        barrier.wait()
        results.append(
            repo.claim(
                "t1",
                expected_statuses=["SCHEDULED"],
                data={"status": "RELEASING", "claimed_by": name},
                umu_id="u1",
            )
        )

    threads = [threading.Thread(target=worker, args=(n,)) for n in ("a", "b")]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    winners = [r for r in results if r is not None]
    assert len(results) == 2 and len(winners) == 1
    assert real_collection.find_one({"_id": "t1"})["claimed_by"] == winners[0]["claimed_by"]
