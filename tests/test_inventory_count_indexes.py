import mongomock

from pharmagob.mongodb_repositories import inventory_count_indexes as indexes


def test_creates_every_index_and_is_idempotent():
    db = mongomock.MongoClient().db
    first = indexes.create_inventory_count_indexes(db)
    second = indexes.create_inventory_count_indexes(db)
    assert first == second
    assert set(first) == set(indexes.INDEXES)
    assert "reference_id_unique" in first[indexes.TASKS]
    assert len(first[indexes.EVENTS]) == 3


def test_line_lock_is_unique_and_partial_on_open_lines():
    lock = next(
        index
        for index in indexes.INDEXES[indexes.LINES]
        if index.document["name"] == "open_line_lock_by_count_type"
    )
    assert lock.document["unique"] is True
    assert lock.document["partialFilterExpression"] == {"is_open": True}
    assert list(lock.document["key"].keys()) == [
        "umu_id",
        "task.count_type",
        "location_content_id",
    ]


def test_task_reference_id_is_unique():
    folio = next(
        index
        for index in indexes.INDEXES[indexes.TASKS]
        if index.document["name"] == "reference_id_unique"
    )
    assert folio.document["unique"] is True
