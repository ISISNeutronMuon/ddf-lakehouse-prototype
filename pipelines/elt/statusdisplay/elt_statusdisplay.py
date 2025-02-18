import logging
import os

import dlt
from dlt.extract import DltSource
from dlt.sources.rest_api import rest_api_source
import humanize

from pipelines_common.constants import SOURCE_NAMESPACE_PREFIX, SOURCE_TABLE_PREFIX
from pipelines_common.destinations.pyiceberg import pyiceberg

DEVEL = os.environ.get("DEVEL", "false") == "true"
LOGGER = logging.getLogger(__name__)
NAMESPACE_NAME = f"{SOURCE_NAMESPACE_PREFIX}statusdisplay"
PIPELINE_NAME = f"{"dev_" if DEVEL else ""}elt_statusdisplay"


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


def extract_and_load():
    pipeline = dlt.pipeline(
        destination=pyiceberg(namespace_name=NAMESPACE_NAME),
        pipeline_name=PIPELINE_NAME,
        progress="log",
    )
    source = statusdisplay()
    for name, resource in source.resources.items():
        resource.apply_hints(table_name=f"{SOURCE_TABLE_PREFIX}{name}")

    pipeline.run(source, write_disposition="replace")
    LOGGER.debug(pipeline.last_trace.last_extract_info)
    LOGGER.debug(pipeline.last_trace.last_load_info)
    LOGGER.info(
        f"Pipeline run completed in {
        humanize.precisedelta(
            pipeline.last_trace.finished_at - pipeline.last_trace.started_at
        )}"
    )
    return pipeline


# ------------------------------------------------------------------------------
# Main
logging.basicConfig(level=logging.INFO)

pipeline = extract_and_load()
# transform(pipeline)
