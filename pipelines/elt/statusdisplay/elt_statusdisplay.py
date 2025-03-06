import logging

import dlt
from dlt.extract import DltSource
from dlt.sources.rest_api import rest_api_source
import humanize

from pipelines_common.pipeline import pipeline_name

# Runtime
LOGGER = logging.getLogger(__name__)
PIPELINE_BASENAME = "statusdisplay"

# Staging destination
LOADER_FILE_FORMAT = "parquet"


def statusdisplay() -> DltSource:
    return rest_api_source(
        name="statusdisplay",
        config={
            "client": {
                "base_url": dlt.config["sources.base_url"],
            },
            "resources": dlt.config["sources.resources"],
        },
    )


def extract_and_load():
    pipeline_name_fq = pipeline_name(PIPELINE_BASENAME)
    LOGGER.info(f"-- Pipeline={pipeline_name_fq} --")

    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name_fq,
        dataset_name=pipeline_name_fq,
        destination="filesystem",
        progress="log",
    )
    source = statusdisplay()
    pipeline.run(source, loader_file_format="jsonl", write_disposition="replace")
    LOGGER.debug(pipeline.last_trace.last_extract_info)
    LOGGER.debug(pipeline.last_trace.last_load_info)
    LOGGER.info(
        f"Pipeline {pipeline_name_fq} completed in {
        humanize.precisedelta(
            pipeline.last_trace.finished_at - pipeline.last_trace.started_at
        )}"
    )
    return pipeline


def configure_logging(root_level: int):
    class FilterUnwantedRecords:
        def filter(self, record):
            return record.name == "__main__"

    logging.basicConfig(level=root_level)
    for handler in logging.getLogger().handlers:
        handler.addFilter(FilterUnwantedRecords())


def main():
    configure_logging(logging.DEBUG)
    extract_and_load()


# ------------------------------------------------------------------------------
if __name__ == "__main__":
    main()
