from typing import cast

import dlt
from dlt.common.typing import TDataItems
from dlt.common.schema import TTableSchema
import pyarrow.parquet as pq

import pipelines_common.utils.iceberg as iceberg


@dlt.destination(
    name="pyiceberg",
    batch_size=0,
    loader_file_format="parquet",
    naming_convention="snake_case",
    max_table_nesting=1,
)
def pyiceberg(filename: TDataItems, table: TTableSchema) -> None:
    """Dlt destination using pyiceberg to write tables to an Iceberg catalog.

    This currently only supports the `replace` write_disposition as it does not understand
    schema evolution.

    :param filename: The filename to the extracted package of data in parquet format
    :param table: A dictionary describing the table schema
    :raises ValueError: if write_disposition for the table is not `replace`
    """
    assert "name" in table
    assert "write_disposition" in table

    table_name = cast(str, table["name"])
    write_disposition = cast(str, table["write_disposition"])

    if write_disposition != "replace":
        raise ValueError(
            "`write_disposition must be `replace`,"
            f" but `{write_disposition}` was provided."
        )

    catalog = iceberg.catalog_get_or_create(
        dlt.config["destination.catalog_name"],
        dlt.config["destination.catalog_uri"],
    )
    namespace = dlt.config["destination.namespace_name"]
    catalog.create_namespace_if_not_exists(namespace)

    # batch=0 gives filename of extracted data in loader_file_format
    extracted_table_data = pq.read_table(filename)
    destination_table = catalog.create_table_if_not_exists(
        iceberg.table_identifier(namespace, table_name),
        extracted_table_data.schema,
    )
    destination_table.overwrite(extracted_table_data)
