from collections.abc import Generator, Sequence
from pathlib import Path

import dlt
from dlt import Pipeline
from dlt.common.pipeline import LoadInfo
from dlt.sources import DltResource
from dlt.sources.sql_database import sql_database
import humanize
import pendulum

import pipelines_common.cli as cli_utils
from pipelines_common.destinations import raise_if_destination_not
import pipelines_common.destinations.filesystem as filesystem_utils
import pipelines_common.logging as logging_utils
import pipelines_common.pipeline as pipeline_utils
import pipelines_common.spark as spark_utils

# Runtime
LOGGER = logging_utils.logging.getLogger(__name__)
PIPELINE_BASENAME = "opralog"
SPARK_APPNAME = Path(__file__).name

# Staging destination
LOADER_FILE_FORMAT = "parquet"

# Transforms
MODELS_DIR = Path(__file__).parent / "models"


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
    LOGGER.info("Running pipeline")
    load_info = pipeline.run(
        opralog(), loader_file_format=LOADER_FILE_FORMAT, write_disposition="append"
    )
    LOGGER.debug(load_info)
    LOGGER.info(
        f"Pipeline {pipeline.pipeline_name} run completed in {
        humanize.precisedelta(
            pipeline.last_trace.finished_at - pipeline.last_trace.started_at
        )}"
    )

    return load_info


def transform(pipeline: Pipeline):
    """Create/update models based on the loaded data.

    This currently recomputes the whole model each time"""
    LOGGER.info("Running transformations to create models")
    raise_if_destination_not("filesystem", pipeline)

    transform_started_at = pendulum.now()
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
    spark.sql(f"USE {catalog_name}.{namespace_name}")

    # Create table
    model_table_name = "equipment_downtime_record"
    LOGGER.debug(f"Creating {model_table_name}...")
    spark_utils.execute_sql_from_file(spark, MODELS_DIR / f"{model_table_name}_def.sql")

    # Create temporary tables required by transform query
    for temp_table in (
        "entries",
        "logbook_entries",
        "logbook_chapter",
        "logbooks",
        "more_entry_columns",
        "additional_columns",
    ):
        (table_dir,) = filesystem_utils.get_table_dirs(pipeline, [temp_table])
        df = spark_utils.read_all_parquet(spark, table_dir)
        df.createOrReplaceTempView(temp_table)

    model_table_df = spark_utils.execute_sql_from_file(
        spark, MODELS_DIR / f"{model_table_name}.sql"
    )
    spark.sql(f"DELETE FROM {model_table_name}")
    LOGGER.debug(
        f"Inserting {model_table_df.count()} new records into model '{model_table_name}'"
    )
    model_table_df.write.insertInto(model_table_name)
    LOGGER.debug(f"Table {model_table_name} updated.")

    transform_ended_at = pendulum.now()
    LOGGER.info(
        f"Completed transformations in {humanize.precisedelta(transform_ended_at - transform_started_at)}"
    )


# ------------------------------------------------------------------------------
def main():
    args = cli_utils.create_standard_argparser().parse_args()
    logging_utils.configure_logging(args.log_level, keep_records_from=["__main__"])

    pipeline = pipeline_utils.create_pipeline(
        PIPELINE_BASENAME, destination="filesystem", progress="log"
    )
    LOGGER.info(f"-- Pipeline={pipeline.pipeline_name} --")

    if args.skip_extract_and_load:
        LOGGER.info("Skpping extract and load step as requested.")
    else:
        extract_and_load_opralog(pipeline)

    # Flush all logs at this point. Other tools such as Spark don't use Python
    # logging and logs can then appear out of order as the other logging is written
    # before the Python log is flushed
    for handler in LOGGER.handlers:
        handler.flush()

    if args.skip_transform:
        LOGGER.info("Skpping transform step as requested.")
    else:
        transform(pipeline)


if __name__ == "__main__":
    main()
