from collections.abc import Generator, Sequence
from pathlib import Path

import dlt
from dlt import Pipeline
from dlt.common.pipeline import LoadInfo
from dlt.sources import DltResource
from dlt.sources.sql_database import sql_database
import humanize

import pipelines_common.cli as cli_utils
from pipelines_common.destinations import raise_if_destination_not
import pipelines_common.logging as logging_utils
import pipelines_common.pipeline as pipeline_utils
import pipelines_common.spark as spark_utils

# Runtime
LOGGER = logging_utils.logging.getLogger(__name__)
PIPELINE_BASENAME = "opralog"
SPARK_APPNAME = Path(__file__).name

# Staging destination
LOADER_FILE_FORMAT = "parquet"


@dlt.source(name="opralog")
def opralog() -> Generator[DltResource]:
    """Pull the configured tables from the database backing the Opralog application"""
    tables = dlt.config["sources.sql_database.tables"]
    source = sql_database(
        schema=dlt.config.value,
        backend="pyarrow",
        backend_kwargs={"tz": "UTC"},
    ).with_resources(*(table_info["name"] for table_info in tables))
    for table_info in tables:
        resource = getattr(source, table_info["name"])
        resource.apply_hints(
            incremental=dlt.sources.incremental(table_info["incremental_id"])
        )
        yield resource


def extract_and_load_opralog(pipeline: Pipeline) -> LoadInfo:
    load_info = pipeline.run(
        opralog(), loader_file_format=LOADER_FILE_FORMAT, write_disposition="append"
    )
    LOGGER.info(load_info)
    LOGGER.info(
        f"Pipeline {pipeline.pipeline_name} run completed in {
        humanize.precisedelta(
            pipeline.last_trace.finished_at - pipeline.last_trace.started_at
        )}"
    )

    return load_info


def transform(pipeline: Pipeline, loads_ids: Sequence[str]):
    """Given loaded data apply transformations to the data"""
    raise_if_destination_not("filesystem", pipeline)

    LOGGER.debug(f"Using load packages '{loads_ids}' as transform sources.")

    spark = spark_utils.create_spark_session(
        app_name=SPARK_APPNAME,
        controller_url=dlt.secrets["transform.spark.controller_url"],
        config=dlt.secrets["transform.spark.config"],
    )

    catalog_name, namespace_name = (
        dlt.config["transform.catalog_name"],
        dlt.config["transform.namespace_name"],
    )
    spark_utils.create_namespace_if_not_exists(spark, catalog_name, namespace_name)


def ensure_equipment_downtime_table_exists(
    spark: spark_utils.SparkSession, table_id: str
):
    edrtable_ensure_exists = f"""
        CREATE TABLE IF NOT EXISTS {table_id} (
        entry_id LONG,
        time_logged TIMESTAMP,
        cycle_name STRING,
        interval_type STRING,
        interval_label STRING,
        equipment STRING,
        downtime_mins DOUBLE,
        group STRING,
        comment_text STRING
        )
        USING iceberg
        PARTITIONED BY (month(time_logged), equipment)
    """
    spark.sql(edrtable_ensure_exists)


# ------------------------------------------------------------------------------
def main():
    args = cli_utils.create_standard_argparser().parse_args()
    logging_utils.configure_logging(args.log_level, keep_records_from=["__main__"])

    pipeline = pipeline_utils.create_pipeline(
        PIPELINE_BASENAME, destination="filesystem", progress="log"
    )
    LOGGER.info(f"-- Pipeline={pipeline.pipeline_name} --")

    extract_and_load_opralog(pipeline)


if __name__ == "__main__":
    main()
