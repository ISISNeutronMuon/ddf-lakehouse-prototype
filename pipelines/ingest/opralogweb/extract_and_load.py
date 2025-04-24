from collections.abc import Generator, Iterator, Sequence

import dlt
from dlt.sources import DltResource
from dlt.sources.sql_database import sql_database

import pipelines_common.cli as cli_utils

# Staging destination
LOADER_FILE_FORMAT = "parquet"


@dlt.source()
def opralogwebdb() -> Generator[DltResource]:
    """Pull the configured tables from the database backing the Opralog application"""
    tables = dlt.config["sources.sql_database.tables"]
    source = sql_database(
        schema=dlt.config.value,
        backend="pyarrow",
        backend_kwargs={"tz": "UTC"},
        table_names=[table_info["name"] for table_info in tables],
    )
    for table_info in tables:
        resource = getattr(source, table_info["name"])
        resource.apply_hints(
            incremental=dlt.sources.incremental(table_info["incremental_id"])
        )
        yield resource


# ------------------------------------------------------------------------------

if __name__ == "__main__":
    cli_utils.cli_main(
        pipeline_name="opralogwebdb",
        data=opralogwebdb(),
        default_write_disposition="append",
    )
