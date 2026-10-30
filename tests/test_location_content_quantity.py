import threading

import mongomock
import pytest
from pymongo.errors import DuplicateKeyError

from pharmagob.mongodb_repositories.base import BaseMongoDbRepository
from pharmagob.mongodb_repositories.location_contents import LocationContentRepository

AUTHOR = {"id": "a1", "umu_id": "u1", "display_name": "Ana"}


def new_document(**overrides):
    document = {
        "_id": "lc1",
        "umu_id": "u1",
        "lot": "L1",
        "expiration_date": 99,
        "quantity": 10,
        "item": {"id": "i1"},
        "location": {"id": "loc1", "label_code": "FARB"},
        "shipment_details": None,
        "created_at": 1,
        "updated_at": 1,
        "version": "1.0.0",
        "last_author": {"id": "old", "umu_id": "u1", "display_name": "Old"},
    }
    document.update(overrides)
    return document


@pytest.fixture
def real_collection():
    return mongomock.MongoClient().db.location_contents


@pytest.fixture
def repo(mocker, real_collection):
    manager = mocker.MagicMock()
    manager.get_collection.return_value = real_collection
    return LocationContentRepository(manager, "location-contents")


@pytest.fixture
def mocked_repo(db_manager):
    return LocationContentRepository(db_manager, "location-contents")


def decrement(repo, quantity, **kwargs):
    return repo.increment_quantity(
        "lc1",
        -quantity,
        min_quantity=quantity,
        last_author=AUTHOR,
        updated_at=500,
        **kwargs,
    )


def test_decrement_with_enough_stock_returns_the_document_after(repo, real_collection):
    real_collection.insert_one(new_document(quantity=10))
    after = decrement(repo, 4)
    assert after["quantity"] == 6
    assert after["last_author"] == AUTHOR and after["updated_at"] == 500
    stored = real_collection.find_one({"_id": "lc1"})
    assert stored["quantity"] == 6
    assert stored["created_at"] == 1 and stored["item"] == {"id": "i1"}


def test_decrement_of_exactly_the_stock_leaves_zero(repo, real_collection):
    real_collection.insert_one(new_document(quantity=3))
    assert decrement(repo, 3)["quantity"] == 0


def test_decrement_without_enough_stock_returns_none_and_writes_nothing(
    repo, real_collection
):
    real_collection.insert_one(new_document(quantity=2))
    assert decrement(repo, 3) is None
    assert real_collection.find_one({"_id": "lc1"})["quantity"] == 2
    assert real_collection.find_one({"_id": "lc1"})["last_author"]["id"] == "old"


def test_decrement_of_a_missing_location_content_returns_none(repo):
    assert decrement(repo, 1) is None


def test_decrement_of_another_umu_returns_none(repo, real_collection):
    real_collection.insert_one(new_document(quantity=10))
    assert decrement(repo, 1, umu_id="u2") is None
    assert decrement(repo, 1, umu_id="u1")["quantity"] == 9


def test_increment_needs_no_condition(repo, real_collection):
    real_collection.insert_one(new_document(quantity=0))
    after = repo.increment_quantity(
        "lc1", 5, last_author=AUTHOR, updated_at=500
    )
    assert after["quantity"] == 5


def test_increment_of_a_missing_location_content_does_not_create_it(
    repo, real_collection
):
    assert repo.increment_quantity("lc1", 5, last_author=AUTHOR, updated_at=1) is None
    assert real_collection.count_documents({}) == 0


@pytest.mark.parametrize(
    "delta,min_quantity",
    [(0, None), (-3, None), (-3, 2), (-3, 0)],
)
def test_invalid_deltas_are_rejected(delta, min_quantity, mocked_repo, collection):
    with pytest.raises(ValueError):
        mocked_repo.increment_quantity(
            "lc1",
            delta,
            min_quantity=min_quantity,
            last_author=AUTHOR,
            updated_at=1,
        )
    collection.find_one_and_update.assert_not_called()


def test_increment_sends_inc_and_set_without_upsert(mocked_repo, collection):
    mocked_repo.increment_quantity(
        "lc1", -2, min_quantity=2, last_author=AUTHOR, updated_at=7, umu_id="u1"
    )
    args, kwargs = collection.find_one_and_update.call_args
    assert args[0] == {"_id": "lc1", "umu_id": "u1", "quantity": {"$gte": 2}}
    assert args[1] == {
        "$inc": {"quantity": -2},
        "$set": {"last_author": AUTHOR, "updated_at": 7},
    }
    assert kwargs["upsert"] is False
    assert kwargs["return_document"] is True  # ReturnDocument.AFTER


def test_two_threads_decrementing_the_last_unit_only_one_wins(repo, real_collection):
    real_collection.insert_one(new_document(quantity=1))
    barrier, out = threading.Barrier(2), []

    def worker():
        barrier.wait()
        out.append(decrement(repo, 1))

    threads = [threading.Thread(target=worker) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert len([o for o in out if o is not None]) == 1
    assert real_collection.find_one({"_id": "lc1"})["quantity"] == 0


def upsert(repo, delta, **overrides):
    document = new_document(quantity=0, **overrides)
    return repo.upsert_increment_quantity(
        document["_id"],
        delta,
        insert_data=document,
        last_author=AUTHOR,
        updated_at=500,
    )


def test_upsert_creates_the_document_with_quantity_equal_to_delta(
    repo, real_collection
):
    after = upsert(repo, 7)
    assert after["quantity"] == 7
    assert after["last_author"] == AUTHOR and after["updated_at"] == 500
    stored = real_collection.find_one({"_id": "lc1"})
    assert stored["created_at"] == 1 and stored["version"] == "1.0.0"
    assert stored["item"] == {"id": "i1"} and stored["expiration_date"] == 99


def test_upsert_adds_to_an_existing_document_without_touching_the_rest(
    repo, real_collection
):
    real_collection.insert_one(
        new_document(quantity=10, expiration_date=50, item={"id": "kept"}, created_at=3)
    )
    after = upsert(repo, 5, expiration_date=999, item={"id": "ignored"})
    assert after["quantity"] == 15
    assert after["expiration_date"] == 50
    assert after["item"] == {"id": "kept"} and after["created_at"] == 3
    assert after["updated_at"] == 500


@pytest.mark.parametrize("delta", [0, -1])
def test_upsert_only_increments(delta, mocked_repo, collection):
    with pytest.raises(ValueError):
        upsert(mocked_repo, delta)
    collection.find_one_and_update.assert_not_called()


def test_upsert_keeps_quantity_out_of_set_on_insert(mocked_repo, collection):
    upsert(mocked_repo, 3)
    update = collection.find_one_and_update.call_args.args[1]
    inserted = update["$setOnInsert"]
    assert not {"_id", "quantity", "last_author", "updated_at"} & set(inserted)
    assert {"umu_id", "lot", "item", "location", "created_at", "version"} <= set(inserted)
    assert collection.find_one_and_update.call_args.kwargs["upsert"] is True


def test_upsert_retries_once_on_duplicate_key(mocked_repo, collection):
    collection.find_one_and_update.side_effect = [
        DuplicateKeyError("E11000"),
        {"_id": "lc1", "quantity": 8},
    ]
    assert upsert(mocked_repo, 3)["quantity"] == 8
    assert collection.find_one_and_update.call_count == 2


def test_upsert_raises_the_second_duplicate_key(mocked_repo, collection):
    collection.find_one_and_update.side_effect = DuplicateKeyError("E11000")
    with pytest.raises(DuplicateKeyError):
        upsert(mocked_repo, 3)


def test_concurrent_upserts_add_up(repo, real_collection):
    barrier = threading.Barrier(10)

    def worker():
        barrier.wait()
        upsert(repo, 2)

    threads = [threading.Thread(target=worker) for _ in range(10)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert real_collection.find_one({"_id": "lc1"})["quantity"] == 20


def test_update_if_wins_once(db_manager, real_collection, mocker):
    db_manager.get_collection.return_value = real_collection
    repo = BaseMongoDbRepository(db_manager, "stock-transfers")
    real_collection.insert_one({"_id": "st1", "last_event": "CREATED"})
    first = repo.update_if(
        "st1", expected={"last_event": "CREATED"}, data={"last_event": "DISPATCHED"}
    )
    second = repo.update_if(
        "st1", expected={"last_event": "CREATED"}, data={"last_event": "DISPATCHED"}
    )
    assert (first, second) == (1, 0)
    assert real_collection.find_one({"_id": "st1"})["last_event"] == "DISPATCHED"


def test_update_if_counts_a_win_even_if_the_values_do_not_change(
    db_manager, real_collection
):
    db_manager.get_collection.return_value = real_collection
    repo = BaseMongoDbRepository(db_manager, "stock-transfers")
    real_collection.insert_one({"_id": "st1", "last_event": "CREATED"})
    assert repo.update_if("st1", expected={}, data={"last_event": "CREATED"}) == 1


def test_update_if_supports_ne_and_misses_unknown_ids(db_manager, real_collection):
    db_manager.get_collection.return_value = real_collection
    repo = BaseMongoDbRepository(db_manager, "dispatch-records")
    real_collection.insert_one({"_id": "d1", "status": "DISPATCHED"})
    cancel = {"status": "CANCELLED"}
    assert repo.update_if("d1", expected={"status": {"$ne": "CANCELLED"}}, data=cancel) == 1
    assert repo.update_if("d1", expected={"status": {"$ne": "CANCELLED"}}, data=cancel) == 0
    assert repo.update_if("nope", expected={}, data=cancel) == 0


def test_two_threads_update_if_only_one_wins(db_manager, real_collection):
    db_manager.get_collection.return_value = real_collection
    repo = BaseMongoDbRepository(db_manager, "stock-transfers")
    real_collection.insert_one({"_id": "st1", "last_event": "CREATED"})
    barrier, out = threading.Barrier(2), []

    def worker():
        barrier.wait()
        out.append(
            repo.update_if(
                "st1",
                expected={"last_event": "CREATED"},
                data={"last_event": "DISPATCHED"},
            )
        )

    threads = [threading.Thread(target=worker) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert sorted(out) == [0, 1]
