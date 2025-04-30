from types import TracebackType
from typing import Iterable, Optional, Type

from dlt.common.destination.client import (
    JobClientBase,
    WithStateSync,
)

from dlt.common.destination.client import LoadJob, RunnableLoadJob, HasFollowupJobs
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.typing import PreparedTableSchema
from dlt.common.schema import Schema

from pyiceberg.catalog.rest import RestCatalog

from pipelines_common.dlt_destinations.iceberg.configuration import (
    IcebergClientConfiguration,
)


class IcebergLoadJob(RunnableLoadJob):
    def __init__(
        self,
        table: PreparedTableSchema,
        file_path: str,
    ) -> None:
        super().__init__(file_path)

    def run(self):
        raise NotImplementedError("IcebergLoadJob.run")


# Need to add WithStateSync to be able to sync back pipeline state
class IcebergClient(JobClientBase):
    def __init__(
        self,
        schema: Schema,
        config: IcebergClientConfiguration,
        capabilities: DestinationCapabilitiesContext,
    ):
        super().__init__(schema, config, capabilities)
        self.config = config
        self.dataset_name = self.config.normalize_dataset_name(self.schema)
        self._iceberg_catalog = RestCatalog(name="dlt", **config.credentials)

    def __enter__(self) -> "JobClientBase":
        return self

    def __exit__(
        self,
        exc_type: Type[BaseException],
        exc_val: BaseException,
        exc_tb: TracebackType,
    ) -> None:
        pass

    def initialize_storage(
        self, truncate_tables: Optional[Iterable[str]] = None
    ) -> None:
        """Prepares storage to be used ie. creates database schema or file system folder. Truncates requested tables."""
        if not self.is_storage_initialized():
            self._iceberg_catalog.create_namespace(self.dataset_name)
        elif truncate_tables:
            for table_name in truncate_tables:
                table_id = f"{self.dataset_name}.{table_name}"
                if self._iceberg_catalog.table_exists(table_id):
                    self._iceberg_catalog.load_table(table_id).delete()

    def is_storage_initialized(self) -> bool:
        """Returns if storage is ready to be read/written."""
        return self._iceberg_catalog.namespace_exists(self.dataset_name)

    def drop_storage(self) -> None:
        """Brings storage back into not initialized state. Typically data in storage is destroyed."""
        # First purge all tables
        for table_id in self._iceberg_catalog.list_tables(self.dataset_name):
            self._iceberg_catalog.purge_table(table_id)

        # Now drop namespace
        self._iceberg_catalog.drop_namespace(self.dataset_name)

    def create_load_job(
        self,
        table: PreparedTableSchema,
        file_path: str,
        load_id: str,
        restore: bool = False,
    ) -> LoadJob:
        """Creates a load job for a particular `table` with content in `file_path`. Table is already prepared to be loaded."""
        return IcebergLoadJob(table, file_path)

    def complete_load(self, load_id: str) -> None:
        """Marks the load package with `load_id` as completed in the destination. Before such commit is done, the data with `load_id` is invalid."""
        raise NotImplementedError()

    # iceberg helpers
    def _table_id(self, table_name: str) -> str:
        return f"{self.dataset_name}.{table_name}"


# "complete_load", "create_load_job", "drop_storage",
# "get_stored_schema", "get_stored_schema_by_hash", "get_stored_state",
