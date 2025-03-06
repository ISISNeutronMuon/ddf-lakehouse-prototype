import logging

import dlt
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


def extract_and_load():
    pipeline_name_fq = pipeline_name(PIPELINE_BASENAME)
    LOGGER.info(f"-- Pipeline={pipeline_name_fq} --")

    pipeline = dlt.pipeline(
        destination="filesystem",
        pipeline_name=pipeline_name_fq,
        dataset_name=pipeline_name_fq,
        progress="log",
    )

    # Table names are configured in config.toml
    source = sql_database(
        schema=dlt.config.value,
        table_names=dlt.config.value,
        backend="pyarrow",
        backend_kwargs={"tz": "UTC"},
    )
    info = pipeline.run(
        source, loader_file_format=LOADER_FILE_FORMAT, write_disposition="replace"
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
