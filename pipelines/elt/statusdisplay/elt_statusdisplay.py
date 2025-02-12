import logging
from typing import cast

import dlt
from dlt.extract import DltSource
from dlt.sources.rest_api import rest_api_source
from dlt.common.typing import TDataItems
from dlt.common.schema import TTableSchema

import pyarrow.parquet as pq

from pipelines_common.destinations import iceberg

# Constants
PIPELINE_NAME = "elt_opralog"


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

    catalog = iceberg.get_catalog(
        dlt.config["destination.catalog_name"],
        dlt.config["destination.catalog_uri"],
    )
    namespace = dlt.config["destination.namespace_name"]
    if write_disposition == "replace":
        iceberg.drop_table(catalog, namespace, table_name)
    else:
        raise ValueError(
            "`write_disposition must be `replace`,"
            f" but `{write_disposition}` was provided."
        )

    # batch=0 gives filename of extracted data
    extracted_table_data = pq.read_table(filename)
    destination_table = iceberg.create_table(
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
    pipeline_name=PIPELINE_NAME,
    dataset_name=dlt.config["destination.namespace_name"],
)
load_info = pipeline.run(statusdisplay(), write_disposition="replace")
logger.info(load_info)
