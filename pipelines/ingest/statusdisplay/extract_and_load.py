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
        data=statusdisplay(),
        dataset_name="statusdisplay_api",
        default_write_disposition="replace",
    )
