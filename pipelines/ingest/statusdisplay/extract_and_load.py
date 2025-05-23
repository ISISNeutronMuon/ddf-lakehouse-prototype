#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "pipelines-common",
# ]
#
# [tool.uv.sources]
# pipelines-common = { path = "../../pipelines-common" }
# ///

import dlt
from dlt.extract import DltSource
from dlt.sources.rest_api import rest_api_source

import pipelines_common.cli as cli_utils


def statusdisplay() -> DltSource:
    return rest_api_source(
        name="api",
        config={
            "client": {
                "base_url": dlt.config["sources.base_url"],
            },
            "resources": dlt.config["sources.resources"],
        },
    )


# ------------------------------------------------------------------------------
if __name__ == "__main__":
    cli_utils.cli_main(
        pipeline_name="statusdisplay",
        default_destination="pipelines_common.dlt_destinations.pyiceberg",
        data=statusdisplay(),
        dataset_name="sources_statusdisplay_api",
        default_write_disposition="replace",
    )
