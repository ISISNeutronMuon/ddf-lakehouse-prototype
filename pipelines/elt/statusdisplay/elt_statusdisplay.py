import logging

import dlt
from dlt.extract import DltSource
from dlt.sources.rest_api import rest_api_source
import humanize

from pipelines_common.destinations.pyiceberg import pyiceberg

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
logger.info(
    f"Pipeline run completed in {
    humanize.precisedelta(
        pipeline.last_trace.finished_at - pipeline.last_trace.started_at
    )}"
)
