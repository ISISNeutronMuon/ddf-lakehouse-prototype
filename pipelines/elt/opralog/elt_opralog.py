import logging

import dlt
from dlt.sources.sql_database import sql_database
import humanize

from pipelines_common.constants import SOURCE_NAMESPACE_PREFIX, SOURCE_TABLE_PREFIX
from pipelines_common.destinations.pyiceberg import pyiceberg

PIPELINE_NAME = "elt_opralog"
NAMESPACE_NAME = f"{SOURCE_NAMESPACE_PREFIX}opralog"


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    pipeline = dlt.pipeline(
        destination=pyiceberg(namespace_name=NAMESPACE_NAME),
        pipeline_name=PIPELINE_NAME,
        progress="log",
    )

    # Table names are configured in config.toml
    source = sql_database(
        table_names=dlt.config.value,
        backend="pyarrow",
        backend_kwargs={"tz": "UTC"},
    )
    for table_name in dlt.config["sources.sql_database.table_names"]:
        getattr(source, table_name).apply_hints(
            table_name=f"{SOURCE_TABLE_PREFIX}{table_name.lower()}"
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
