import threading

import mongomock
import pytest
from pymongo.errors import DuplicateKeyError

from pharmagob.mongodb_repositories.reference_counters import (
    COLLECTION_NAME,
    ReferenceCounterRepository,
)


@pytest.fixture
def real_collection():
    return mongomock.MongoClient().db.counters


@pytest.fixture
def repo(mocker, real_collection):
    manager = mocker.MagicMock()
    manager.get_collection.return_value = real_collection
    return ReferenceCounterRepository(manager)


def test_binds_the_shared_collection(db_manager):
    ReferenceCounterRepository(db_manager)
    db_manager.get_collection.assert_called_once_with(COLLECTION_NAME)


def test_build_key_with_and_without_scope():
    assert ReferenceCounterRepository.build_key("tasks", "CF") == "tasks:CF"
    assert ReferenceCounterRepository.build_key("tasks", "CF", "2026") == "tasks:CF:2026"


def test_first_use_creates_the_document_and_returns_one(repo, real_collection):
    assert repo.next_sequence("tasks", "CF", updated_at=10) == 1
    assert real_collection.find_one({"_id": "tasks:CF"}) == {
        "_id": "tasks:CF",
        "seq": 1,
        "updated_at": 10,
        "entity": "tasks",
        "prefix": "CF",
        "scope": None,
    }


def test_consecutive_calls_increment(repo, real_collection):
    values = [repo.next_sequence("tasks", "CF", updated_at=i) for i in range(3)]
    assert values == [1, 2, 3]
    assert real_collection.find_one({"_id": "tasks:CF"})["updated_at"] == 2


def test_scope_starts_its_own_sequence(repo):
    assert repo.next_sequence("tasks", "CF", updated_at=1) == 1
    assert repo.next_sequence("tasks", "CF", scope="2027", updated_at=1) == 1
    assert repo.next_sequence("tasks", "CF", updated_at=1) == 2


def test_retries_once_on_duplicate_key(db_manager, collection):
    repo = ReferenceCounterRepository(db_manager)
    collection.find_one_and_update.side_effect = [
        DuplicateKeyError("E11000"),
        {"_id": "tasks:CF", "seq": 2},
    ]
    assert repo.next_sequence("tasks", "CF", updated_at=1) == 2
    assert collection.find_one_and_update.call_count == 2


def test_second_duplicate_key_is_raised(db_manager, collection):
    repo = ReferenceCounterRepository(db_manager)
    collection.find_one_and_update.side_effect = DuplicateKeyError("E11000")
    with pytest.raises(DuplicateKeyError):
        repo.next_sequence("tasks", "CF", updated_at=1)


def test_twenty_threads_get_distinct_sequences(repo):
    barrier = threading.Barrier(20)
    values = []

    def worker():
        barrier.wait()
        values.append(repo.next_sequence("tasks", "CF", updated_at=1))

    threads = [threading.Thread(target=worker) for _ in range(20)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert sorted(values) == list(range(1, 21))
