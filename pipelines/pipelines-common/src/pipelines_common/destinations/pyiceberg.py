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

    write_disposition = cast(str, table["write_disposition"])
    if write_disposition not in ("append", "replace", "skip"):
        raise ValueError(
            "`write_disposition must be one of (`append`, `replace`, `skip`)"
            f" but `{write_disposition}` was provided."
        )

    catalog = iceberg.catalog_create(catalog_properties)
    table_name = cast(str, table["name"])
    table_id = iceberg.table_identifier(namespace_name, table_name)
    if write_disposition == "skip" and catalog.table_exists(table_id):
        return

    # batch=0 gives filename of extracted data in loader_file_format
    extracted_table_data = pq.read_table(filename)
    partition_spec = table.get("x-partition-spec", None)
    catalog.create_namespace_if_not_exists(namespace_name)

    if catalog.table_exists(table_id):
        loaded_table = catalog.load_table(table_id)
        if write_disposition == "append":
            loaded_table.append(extracted_table_data)
        else:
            loaded_table.overwrite(extracted_table_data)
    else:
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
