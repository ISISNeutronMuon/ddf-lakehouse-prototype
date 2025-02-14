import logging

import dlt
from dlt.extract import DltSource
from dlt.sources.rest_api import rest_api_source
import humanize

from pipelines_common.constants import SOURCE_NAMESPACE_PREFIX, SOURCE_TABLE_PREFIX
from pipelines_common.destinations.pyiceberg import pyiceberg

PIPELINE_NAME = "elt_statusdisplay"
NAMESPACE_NAME = f"{SOURCE_NAMESPACE_PREFIX}statusdisplay"


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


# ------------------------------------------------------------------------------
# Main
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

pipeline = dlt.pipeline(
    destination=pyiceberg(namespace_name=NAMESPACE_NAME),
    pipeline_name=PIPELINE_NAME,
    progress="log",
)
source = statusdisplay()
for name, resource in source.resources.items():
    resource.apply_hints(table_name=f"{SOURCE_TABLE_PREFIX}{name}")

load_info = pipeline.run(source, write_disposition="replace")
logger.info(load_info)
logger.info(
    f"Pipeline run completed in {
    humanize.precisedelta(
        pipeline.last_trace.finished_at - pipeline.last_trace.started_at
    )}"
)
