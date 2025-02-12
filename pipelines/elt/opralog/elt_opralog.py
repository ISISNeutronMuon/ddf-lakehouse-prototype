import logging

import dlt
import humanize

from dlt.sources.sql_database import sql_database

PIPELINE_NAME = "elt_opralog"


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    pipeline = dlt.pipeline(
        destination="filesystem",
        pipeline_name=PIPELINE_NAME,
        progress="log",
        #        dataset_name=dlt.config["destination.namespace_name"],
    )

    # Table names are configured in config.toml
    source = sql_database()
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
