#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.
import logging
import os
from typing import Optional

from testcontainers.core.config import MAX_TRIES
from testcontainers.core.generic import DbContainer
from testcontainers.core.waiting_utils import wait_container_is_ready, wait_for_logs

from cratedb_toolkit.testing.testcontainers.util import DockerSkippingContainer, KeepaliveContainer, asbool

logger = logging.getLogger(__name__)


class CrateDBContainer(DockerSkippingContainer, KeepaliveContainer, DbContainer):
    """
    CrateDB database container.

    Example:

        The example spins up a CrateDB database and connects to it using
        SQLAlchemy and its Python driver.

        .. doctest::

            >>> from cratedb_toolkit.testing.testcontainers.cratedb import CrateDBContainer
            >>> import sqlalchemy

            >>> cratedb_container = CrateDBContainer("crate:5.2.3")
            >>> with cratedb_container as cratedb:
            ...     engine = sqlalchemy.create_engine(cratedb.get_connection_url())
            ...     with engine.begin() as connection:
            ...         result = connection.execute(sqlalchemy.text("select version()"))
            ...         version, = result.fetchone()
            >>> version
            'CrateDB 5.2.3...'
    """

    CRATEDB_USER = os.environ.get("CRATEDB_USER", "crate")
    CRATEDB_PASSWORD = os.environ.get("CRATEDB_PASSWORD", "")
    CRATEDB_DB = os.environ.get("CRATEDB_DB", "doc")
    KEEPALIVE = asbool(os.environ.get("CRATEDB_KEEPALIVE", os.environ.get("TC_KEEPALIVE", False)))

    # TODO: Dual-port use with 4200+5432.
    def __init__(
        self,
        image: str = "crate/crate:nightly",
        port: int = 4200,
        user: Optional[str] = None,
        password: Optional[str] = None,
        dbname: Optional[str] = None,
        dialect: str = "crate",
        **kwargs,
    ) -> None:
        super().__init__(image=image, **kwargs)

        self._name = "testcontainers-cratedb"  # -{os.getpid()}
        self._command = "-Cdiscovery.type=single-node -Ccluster.routing.allocation.disk.threshold_enabled=false"
        # TODO: Generalize by obtaining more_opts from caller.
        self._command += " -Cnode.attr.storage=hot"
        self._command += " -Cpath.repo=/tmp/snapshots"

        self.CRATEDB_USER = user or self.CRATEDB_USER
        self.CRATEDB_PASSWORD = password or self.CRATEDB_PASSWORD
        self.CRATEDB_DB = dbname or self.CRATEDB_DB

        self.port_to_expose = port
        self.dialect = dialect

    def _configure(self) -> None:
        self.with_exposed_ports(self.port_to_expose)
        self.with_env("CRATEDB_USER", self.CRATEDB_USER)
        self.with_env("CRATEDB_PASSWORD", self.CRATEDB_PASSWORD)
        self.with_env("CRATEDB_DB", self.CRATEDB_DB)

    @wait_container_is_ready()
    def get_connection_url(self, host=None) -> str:
        # TODO: When using `db_name=self.CRATEDB_DB`:
        #       Connection.__init__() got an unexpected keyword argument 'database'
        wait_for_logs(self, predicate="o.e.n.Node.*started", timeout=MAX_TRIES)
        return super()._create_connection_url(
            dialect=self.dialect,
            username=self.CRATEDB_USER,
            password=self.CRATEDB_PASSWORD,
            host=host,
            port=self.port_to_expose,
        )

    @wait_container_is_ready()
    def _connect(self):
        # TODO: Better use a network connectivity health check?
        #       In `testcontainers-java`, there is the `HttpWaitStrategy`.
        # TODO: Provide a client instance.
        wait_for_logs(self, predicate="o.e.n.Node.*started", timeout=MAX_TRIES)
