from pathlib import Path
from typing import Sequence

import dlt
from dlt import Pipeline
from dlt.common.pipeline import LoadInfo
from dlt.extract import DltSource
from dlt.sources.rest_api import rest_api_source
import humanize

import pipelines_common.cli as cli_utils
from pipelines_common.destinations import raise_if_destination_not
import pipelines_common.destinations.filesystem as filesystem_utils
import pipelines_common.pipeline as pipeline_utils
import pipelines_common.logging as logging_utils
import pipelines_common.spark as spark_utils

# Runtime
LOGGER = logging_utils.logging.getLogger(__name__)
PIPELINE_BASENAME = "statusdisplay"
SPARK_APPNAME = Path(__file__).name

# Staging destination
LOADER_FILE_FORMAT = "parquet"

# Transforms
MODELS_DIR = Path(__file__).parent / "models"


def statusdisplay() -> DltSource:
    return rest_api_source(
        name="statusdisplay",
        config={
            "client": {
                "base_url": dlt.config["sources.base_url"],
            },
            "resources": dlt.config["sources.resources"],
        },
    )


def extract_and_load_statusdisplay(pipeline: Pipeline) -> LoadInfo:
    """Run the pipeline on the statusdisplay source"""
    source = statusdisplay()
    load_info = pipeline.run(
        source, loader_file_format=LOADER_FILE_FORMAT, write_disposition="replace"
    )

    LOGGER.debug(pipeline.last_trace.last_extract_info)
    LOGGER.debug(pipeline.last_trace.last_load_info)
    LOGGER.info(
        f"Pipeline {pipeline.pipeline_name} completed in {
        humanize.precisedelta(
            pipeline.last_trace.finished_at - pipeline.last_trace.started_at
        )}"
    )
    return load_info


def transform(pipeline: Pipeline, loads_ids: Sequence[str]):
    """Given the loads_ids for a loaded set of tables compute the final schedule table"""
    raise_if_destination_not("filesystem", pipeline)

    LOGGER.info(f"Using load packages '{loads_ids}' as transform sources.")
    # Spark is not really required for this transformation given the data volume
    # but we use it for consistency across all imported sources and it's closer
    # to the data
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
    model_table_name = "running_schedule"
    spark_utils.execute_sql_from_file(spark, MODELS_DIR / f"{model_table_name}_def.sql")

    # Create temporary table from raw source
    (schedule_table_dir,) = filesystem_utils.get_table_dirs(pipeline, ["schedule"])
    schedule_df = spark_utils.read_all_parquet(spark, schedule_table_dir, loads_ids)
    # name is currently duplicated in .sql script...
    schedule_df.createOrReplaceTempView("schedule")
    model_table_df = spark_utils.execute_sql_from_file(
        spark, MODELS_DIR / f"{model_table_name}.sql"
    )

    # We currently replace the whole thing everytime as the data is tiny...
    spark.sql(f"DELETE FROM {model_table_name}")
    model_table_df.write.insertInto(model_table_name)

    LOGGER.info(f"Completed transformations.")


def main():
    args = cli_utils.create_standard_argparser().parse_args()
    logging_utils.configure_logging(args.log_level, keep_records_from=["__main__"])

    pipeline = pipeline_utils.create_pipeline(
        PIPELINE_BASENAME, destination="filesystem", progress="log"
    )
    LOGGER.info(f"-- Pipeline={pipeline.pipeline_name} --")

    if args.skip_extract_and_load:
        loads_ids = pipeline_utils.find_latest_load_id(pipeline)
        if loads_ids is None:
            LOGGER.info("No load ids were found in the destination. Exiting.")
            return

        loads_ids = [loads_ids]
    else:
        load_info = extract_and_load_statusdisplay(pipeline)
        loads_ids = load_info.loads_ids
        if len(loads_ids) == 0:
            LOGGER.info("No data was loaded into the destination. Exiting.")
            return

    transform(pipeline, loads_ids)


# ------------------------------------------------------------------------------
if __name__ == "__main__":
    main()
