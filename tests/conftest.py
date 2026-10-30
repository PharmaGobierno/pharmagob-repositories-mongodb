import pytest


@pytest.fixture
def collection(mocker):
    return mocker.MagicMock()


@pytest.fixture
def db_manager(mocker, collection):
    manager = mocker.MagicMock()
    manager.get_collection.return_value = collection
    return manager
