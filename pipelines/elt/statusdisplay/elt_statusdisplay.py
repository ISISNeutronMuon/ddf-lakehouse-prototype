import logging
from typing import cast, Optional

import dlt
from dlt.extract import DltSource
from dlt.sources.rest_api import rest_api_source
from dlt.common.typing import TDataItems
from dlt.common.schema import TTableSchema

import pyarrow.parquet as pq
import pyiceberg.exceptions as piexc
from pyiceberg.catalog.rest import Catalog, RestCatalog
from pyiceberg.schema import Schema
from pyiceberg.table import Table as IcebergTable

_catalog: Optional[Catalog] = None


# Iceberg
def get_iceberg_catalog(name: str, uri: str) -> Catalog:
    """Connect to an iceberg REST catalog on a given uri"""
    global _catalog
    if _catalog is None:
        _catalog = RestCatalog(name, **{"uri": uri})

    return _catalog


def table_qualified_name(namespace: str, table_name: str) -> str:
    return f"{namespace}.{table_name}"


def create_table(
    catalog: Catalog, namespace: str, table_name: str, table_schema: Schema
) -> IcebergTable:
    """Create a table in the catalog. If the schema does not exist it is created too."""
    try:
        catalog.create_namespace(namespace)
    except piexc.NamespaceAlreadyExistsError:
        pass

    return catalog.create_table(
        table_qualified_name(namespace, table_name), schema=table_schema
    )


def drop_table(catalog: Catalog, namespace: str, table_name: str) -> None:
    """Drops the named table from the namespace in the given catalog. It is a noop if the table does not exist."""
    try:
        catalog.drop_table(table_qualified_name(namespace, table_name))
    except piexc.NoSuchTableError:
        pass


# dlt pipeline
@dlt.source()
def statusdisplay() -> DltSource:
    return rest_api_source(
        config={
            "client": {
                "base_url": dlt.config["sources.base_url"],
            },
            "resources": dlt.config["sources.resources"],
        },
    )


@dlt.destination(
    name="pyiceberg",
    batch_size=0,
    loader_file_format="parquet",
    naming_convention="snake_case",
    max_table_nesting=1,
)
def pyiceberg(filename: TDataItems, table: TTableSchema) -> None:
    assert "name" in table
    assert "write_disposition" in table

    table_name = cast(str, table["name"])
    write_disposition = cast(str, table["write_disposition"])

    catalog = get_iceberg_catalog(
        dlt.config["destination.catalog_name"],
        dlt.config["destination.catalog_uri"],
    )
    namespace = dlt.config["destination.namespace_name"]
    if write_disposition == "replace":
        drop_table(catalog, namespace, table_name)
    else:
        raise ValueError(
            "`write_disposition must be `replace`,"
            f" but `{write_disposition}` was provided."
        )

    # batch=0 gives filename of extracted data
    extracted_table_data = pq.read_table(filename)
    destination_table = create_table(
        catalog,
        namespace,
        table_name,
        extracted_table_data.schema,
    )
    destination_table.append(extracted_table_data)


# ------------------------------------------------------------------------------
# Main
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

pipeline = dlt.pipeline(
    destination=pyiceberg(),
    pipeline_name=dlt.config["destination.catalog_name"],
    dataset_name=dlt.config["destination.namespace_name"],
)
load_info = pipeline.run(statusdisplay(), write_disposition="replace")
logger.info(load_info)
