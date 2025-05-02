from types import TracebackType
from typing import cast, Iterable, Optional, Type

from dlt.common.destination.client import (
    JobClientBase,
    WithStateSync,
)

from dlt.common.destination.client import LoadJob, RunnableLoadJob
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.typing import PreparedTableSchema
from dlt.common.schema import Schema
from dlt.common.schema.utils import loads_table, normalize_table_identifiers

import pendulum
import pyarrow as pa
import pyarrow.parquet as pq

from pyiceberg.catalog.rest import (
    Catalog as PyIcebergCatalog,
    RestCatalog as PyIcebergRestCatalog,
)

from pipelines_common.dlt_destinations.pyiceberg.configuration import (
    IcebergClientConfiguration,
)


class PyIcebergLoadJob(RunnableLoadJob):
    def __init__(
        self,
        file_path: str,
    ) -> None:
        super().__init__(file_path)

    @property
    def iceberg_client(self) -> "PyIcebergClient":
        return cast(PyIcebergClient, self._job_client)

    @property
    def iceberg_catalog(self) -> PyIcebergCatalog:
        return self.iceberg_client.iceberg_catalog

    def run(self):
        qualified_table_name = self.iceberg_client.make_qualified_table_name(
            self.load_table_name
        )
        self.iceberg_client.append_to_table(
            qualified_table_name,
            pq.read_table(self._file_path),
            create_table_if_not_exists=True,
        )


class PyIcebergClient(JobClientBase):
    def __init__(
        self,
        schema: Schema,
        config: IcebergClientConfiguration,
        capabilities: DestinationCapabilitiesContext,
    ):
        super().__init__(schema, config, capabilities)
        # keep normalized definitions of tables for later use
        self.dataset_name = self.config.normalize_dataset_name(self.schema)
        loads_table_ = normalize_table_identifiers(loads_table(), schema.naming)
        self.loads_collection_properties = list(loads_table_["columns"].keys())

        self.config = config
        self.iceberg_catalog = PyIcebergRestCatalog(
            name="default", **config.credentials
        )

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
            self.iceberg_catalog.create_namespace(self.dataset_name)
        elif truncate_tables:
            for table_name in truncate_tables:
                table_id = f"{self.dataset_name}.{table_name}"
                if self.iceberg_catalog.table_exists(table_id):
                    self.iceberg_catalog.load_table(table_id).delete()

    def is_storage_initialized(self) -> bool:
        """Returns if storage is ready to be read/written."""
        return self.iceberg_catalog.namespace_exists(self.dataset_name)

    def drop_storage(self) -> None:
        """Brings storage back into not initialized state. Typically data in storage is destroyed."""
        # First purge all tables
        for table_id in self.iceberg_catalog.list_tables(self.dataset_name):
            self.iceberg_catalog.purge_table(table_id)

        # Now drop namespace
        self.iceberg_catalog.drop_namespace(self.dataset_name)

    def create_load_job(
        self,
        table: PreparedTableSchema,
        file_path: str,
        load_id: str,
        restore: bool = False,
    ) -> LoadJob:
        """Creates a load job for a particular `table` with content in `file_path`. Table is already prepared to be loaded."""
        return PyIcebergLoadJob(file_path)

    def complete_load(self, load_id: str) -> None:
        """Marks the load package with `load_id` as completed in the destination. Before such commit is done, the data with `load_id` is invalid."""
        qualified_table_name = self.make_qualified_table_name(
            self.schema.loads_table_name
        )
        values = [
            load_id,
            self.schema.name,
            0,
            pendulum.now(),
            self.schema.version_hash,
        ]
        assert len(values) == len(self.loads_collection_properties)

        data_to_load = pa.Table.from_pylist(
            [{k: v for k, v in zip(self.loads_collection_properties, values)}]
        )
        self.append_to_table(
            qualified_table_name, data_to_load, create_table_if_not_exists=True
        )

    # "get_stored_schema", "get_stored_schema_by_hash", "get_stored_state",

    def make_qualified_table_name(self, table_name: str) -> str:
        return f"{self.dataset_name}.{table_name}"

    def append_to_table(
        self,
        qualified_table_name: str,
        table_data: pa.Table,
        *,
        create_table_if_not_exists: bool = False,
    ):
        """Append a pyarrow Table to an Iceberg table, optionally creating it if it doesn't exist

        :param qualified_table_name: The fully qualified name of the table
        :param table_data: pyarrow.Table containing the data
        :param create_table_if_not_exists: If True create the table if it does not exist, defaults to False
        """
        if self.iceberg_catalog.table_exists(qualified_table_name):
            table = self.iceberg_catalog.load_table(qualified_table_name)
            table.append(table_data)
        elif create_table_if_not_exists:
            with self.iceberg_catalog.create_table_transaction(
                qualified_table_name,
                table_data.schema,
            ) as txn:
                txn.append(table_data)
