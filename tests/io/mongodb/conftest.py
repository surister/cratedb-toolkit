import logging

import pytest

from cratedb_toolkit.testing.testcontainers.util import PytestTestcontainerAdapter

logger = logging.getLogger(__name__)


# Define databases to be deleted before running each test case.
RESET_DATABASES = [
    "testdrive",
]


class MongoDBFixture(PytestTestcontainerAdapter):
    """
    A little helper wrapping Testcontainer's `MongoDbContainer`.
    """

    def __init__(self):
        from pymongo import MongoClient

        self.container = None
        self.client: MongoClient = None
        super().__init__()

    def setup(self):
        # TODO: Make image name configurable.
        from cratedb_toolkit.testing.testcontainers.mongodb import MongoDbContainerWithKeepalive

        self.container = MongoDbContainerWithKeepalive()
        self.container.start()
        self.client = self.container.get_connection_client()

    def reset(self):
        """
        Drop all databases used for testing.
        """
        for database_name in RESET_DATABASES:
            self.client.drop_database(database_name)

    def get_connection_url(self):
        return self.container.get_connection_url()

    def get_connection_client(self):
        return self.container.get_connection_client()


@pytest.fixture(scope="session")
def mongodb_service():
    """
    Provide an MongoDB service instance to the test suite.
    """
    db = MongoDBFixture()
    db.reset()
    yield db
    db.stop()


@pytest.fixture(scope="function")
def mongodb(mongodb_service):
    """
    Provide a fresh canvas to each test case invocation, by resetting database content.
    """
    mongodb_service.reset()
    yield mongodb_service
