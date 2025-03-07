from collections.abc import Generator
import logging

import dlt
from dlt.sources import DltResource
from dlt.sources.sql_database import sql_database
import humanize

from pipelines_common.pipeline import pipeline_name

# Runtime
LOGGER = logging.getLogger(__name__)
PIPELINE_BASENAME = "opralog"

# Staging destination
LOADER_FILE_FORMAT = "parquet"


def configure_logging(root_level: int):
    class FilterUnwantedRecords:
        def filter(self, record):
            return record.name == "__main__"

    logging.basicConfig(level=root_level)
    for handler in logging.getLogger().handlers:
        handler.addFilter(FilterUnwantedRecords())


@dlt.source(name="opralog")
def opralog() -> Generator[DltResource]:
    """Pull the configured tables from the database backing the Opralog application"""
    tables = dlt.config["sources.sql_database.tables"]
    source = sql_database(
        schema=dlt.config.value,
        backend="pyarrow",
        backend_kwargs={"tz": "UTC"},
    ).with_resources(*(table_info["name"] for table_info in tables))
    for table_info in tables:
        resource = getattr(source, table_info["name"])
        resource.apply_hints(
            incremental=dlt.sources.incremental(table_info["incremental_id"])
        )
        yield resource


def extract_and_load():
    pipeline_name_fq = pipeline_name(PIPELINE_BASENAME)
    LOGGER.info(f"-- Pipeline={pipeline_name_fq} --")

    pipeline = dlt.pipeline(
        destination="filesystem",
        pipeline_name=pipeline_name_fq,
        dataset_name=pipeline_name_fq,
        progress="log",
    )

    info = pipeline.run(
        opralog(), loader_file_format=LOADER_FILE_FORMAT, write_disposition="append"
    )
    LOGGER.info(info)
    LOGGER.info(
        f"Pipeline {pipeline_name_fq} run completed in {
        humanize.precisedelta(
            pipeline.last_trace.finished_at - pipeline.last_trace.started_at
        )}"
    )


def main():
    configure_logging(logging.DEBUG)
    extract_and_load()


# ------------------------------------------------------------------------------

if __name__ == "__main__":
    main()
