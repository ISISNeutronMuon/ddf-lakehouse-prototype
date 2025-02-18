from typing import cast, Dict

import dlt
from dlt.common.typing import TDataItems
from dlt.common.schema import TTableSchema
import pyarrow.parquet as pq
from pyiceberg.transforms import parse_transform

import pipelines_common.utils.iceberg as iceberg


@dlt.destination(
    name="pyiceberg",
    batch_size=0,
    loader_file_format="parquet",
    naming_convention="snake_case",
    max_table_nesting=1,
)
def pyiceberg(
    filename: TDataItems,
    table: TTableSchema,
    namespace_name: str = dlt.config.value,
    catalog_properties: Dict[str, str] = dlt.config.value,
) -> None:
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

    catalog = iceberg.catalog_get_or_create(catalog_properties)
    catalog.create_namespace_if_not_exists(namespace_name)
    table_id = iceberg.table_identifier(namespace_name, table_name)
    if catalog.table_exists(table_id):
        catalog.drop_table(table_id)

    # batch=0 gives filename of extracted data in loader_file_format
    extracted_table_data = pq.read_table(filename)
    table_id = (table_id[0], table_id[1])
    partition_spec = table.get("x-partition-spec", None)

    with catalog.create_table_transaction(
        table_id,
        extracted_table_data.schema,
    ) as txn:
        if partition_spec is not None:
            with txn.update_spec() as update_spec:
                for spec in partition_spec:
                    if isinstance(spec, str):
                        update_spec.add_identity(spec)
                    else:
                        partition_field_name = spec[2] if len(spec) == 3 else None
                        update_spec.add_field(
                            spec[0], parse_transform(spec[1]), partition_field_name
                        )
        # add data
        txn.append(extracted_table_data)
