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
    """Given loaded data apply transformations to the data

    Currently we reread all of the source and compute the whole downstream dataset again
    """
    raise_if_destination_not("filesystem", pipeline)

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
    model_table_df = spark_utils.execute_sql_from_file(
        spark, MODELS_DIR / f"{model_table_name}_def.sql"
    )

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

    # We currently replace the whole thing everytime as the data is tiny...
    spark.sql(f"DELETE FROM {model_table_name}")
    model_table_df.write.insertInto(model_table_name)
    LOGGER.debug(f"Table {model_table_name} updated.")

    LOGGER.info(f"Completed transformations.")


# ------------------------------------------------------------------------------
def main():
    args = cli_utils.create_standard_argparser().parse_args()
    logging_utils.configure_logging(args.log_level, keep_records_from=["__main__"])

    pipeline = pipeline_utils.create_pipeline(
        PIPELINE_BASENAME, destination="filesystem", progress="log"
    )
    LOGGER.info(f"-- Pipeline={pipeline.pipeline_name} --")

    if not args.skip_extract_and_load:
        extract_and_load_opralog(pipeline)

    if not args.skip_transform:
        transform(pipeline)


if __name__ == "__main__":
    main()
