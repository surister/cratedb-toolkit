import abc
import dataclasses
import json
import logging
import typing as t
from abc import abstractmethod

from cratedb_toolkit.api.guide import GuidingTexts
from cratedb_toolkit.cluster.util import deploy_cluster, get_cluster_by_name, get_cluster_info
from cratedb_toolkit.exception import CroudException, OperationFailed
from cratedb_toolkit.io.croud import CloudIo
from cratedb_toolkit.model import ClusterInformation, DatabaseAddress, InputOutputResource, TableAddress

logger = logging.getLogger(__name__)


class ClusterBase(abc.ABC):
    @abstractmethod
    def load_table(self, resource: InputOutputResource, target: TableAddress):
        raise NotImplementedError("Child class needs to implement this method")


@dataclasses.dataclass
class ManagedCluster(ClusterBase):
    """
    Wrap a managed CrateDB database cluster on CrateDB Cloud.
    """

    cloud_id: t.Optional[str] = None
    name: t.Optional[str] = None
    address: t.Optional[DatabaseAddress] = None
    info: t.Optional[ClusterInformation] = None
    exists: bool = False

    def __post_init__(self):
        logger.info(f"Connecting to CrateDB Cloud Cluster: {self.cloud_id} ({self.name})")
        self.probe()

    def probe(self):
        """
        Probe a CrateDB Cloud cluster, API-wise.
        """
        if not self.cloud_id and not self.name:
            self.exists = False
            raise ValueError("Either cluster identifier or name needs to be specified")
        try:
            if self.cloud_id:
                self.info = get_cluster_info(cluster_id=self.cloud_id)
                self.name = self.info.cloud["name"]
            else:
                self.info = get_cluster_by_name(self.name)
                self.cloud_id = self.info.cloud["id"]
        except CroudException as ex:
            self.exists = False
            if "Cluster not found" not in str(ex):
                raise
        if self.info:
            self.exists = True
            logger.info(f"Cluster information: name={self.info.cloud.get('name')}, url={self.info.cloud.get('url')}")

    def acquire(self):
        """
        Acquire a database cluster.
        When cluster does not exist, deploy it.
        """
        if not self.exists:
            logger.info(f"Cluster does not exist: {self.name}")
            logger.info(f"Deploying cluster: {self.name}")
            if self.deploy():
                self.probe()
            else:
                raise CroudException(f"Deployment of cluster failed: {self.name}")

    def deploy(self):
        """
        Run the cluster deployment procedure.
        """
        try:
            deploy_cluster(self.name)
        except CroudException:
            return False
        return True

    def load_table(self, resource: InputOutputResource, target: t.Optional[TableAddress] = None):
        """
        Load data into a database table on CrateDB Cloud.

        Synopsis
        --------
        export CRATEDB_CLOUD_CLUSTER_ID=95998958-4d96-46eb-a77a-a894e7dde128
        ctk load table https://github.com/crate/cratedb-datasets/raw/main/cloud-tutorials/data_weather.csv.gz

        https://console.cratedb.cloud
        """

        target = target or TableAddress()

        try:
            cio = CloudIo(cluster_id=self.cloud_id)
        except CroudException as ex:
            msg = f"Connecting to cluster resource failed: {self.cloud_id}. Reason: {ex}"
            if "Resource not found" in str(ex):
                logger.error(msg)
                return None, False
            logger.exception(msg)
            raise OperationFailed(msg) from ex

        try:
            job_info, success = cio.load_resource(resource=resource, target=target)
            logger.info("Job information:\n%s", json.dumps(job_info, indent=2))
            # TODO: Explicitly report about `failed_records`, etc.
            texts = GuidingTexts(
                admin_url=self.info.cloud["url"],
                table_name=job_info["destination"]["table"],
            )
            if success:
                logger.info("Data loading was successful: %s", texts.success())
                return job_info, success
            else:
                # TODO: Add "reason" to exception message.
                logger.error(f"Data loading failed: {texts.error()}")
                raise OperationFailed("Data loading failed")

            # When exiting so, it is expected that error logging has taken place appropriately.
        except CroudException as ex:
            msg = "Data loading failed: Unknown error"
            logger.exception(msg)
            raise OperationFailed(msg) from ex


@dataclasses.dataclass
class StandaloneCluster(ClusterBase):
    """
    Wrap a standalone CrateDB database cluster.
    """

    address: DatabaseAddress
    info: t.Optional[ClusterInformation] = None

    def load_table(self, resource: InputOutputResource, target: TableAddress):
        """
        Load data into a database table on a standalone CrateDB Server.

        Synopsis
        --------
        export CRATEDB_SQLALCHEMY_URL=crate://crate@localhost:4200/testdrive/demo

        ctk load table influxdb2://example:token@localhost:8086/testdrive/demo
        ctk load table mongodb://localhost:27017/testdrive/demo
        """
        source_url = resource.url
        target_url = self.address.dburi
        if source_url.startswith("influxdb"):
            from cratedb_toolkit.io.influxdb import influxdb_copy

            source_url = source_url.replace("influxdb2://", "http://")
            if not influxdb_copy(source_url, target_url, progress=True):
                msg = "Data loading failed"
                logger.error(msg)
                raise OperationFailed(msg)
        elif source_url.startswith("mongodb"):
            from cratedb_toolkit.io.mongodb.api import mongodb_copy

            if not mongodb_copy(source_url, target_url, progress=True):
                msg = "Data loading failed"
                logger.error(msg)
                raise OperationFailed(msg)
        else:
            raise NotImplementedError("Importing resource not implemented yet")
