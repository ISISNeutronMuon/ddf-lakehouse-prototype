import logging

import dlt
from dlt.sources.sql_database import sql_database
import humanize

from pipelines_common.destinations.pyiceberg import pyiceberg

PIPELINE_NAME = "elt_opralog"


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    pipeline = dlt.pipeline(
        destination=pyiceberg(),
        pipeline_name=PIPELINE_NAME,
        progress="log",
    )

    # Table names are configured in config.toml
    source = sql_database(
        backend="pyarrow",
        backend_kwargs={"tz": "UTC"},
    )
    info = pipeline.run(source, write_disposition="replace")
    logger.info(info)
    logger.info(
        f"Pipeline run completed in {
        humanize.precisedelta(
            pipeline.last_trace.finished_at - pipeline.last_trace.started_at
        )}"
    )


if __name__ == "__main__":
    main()
