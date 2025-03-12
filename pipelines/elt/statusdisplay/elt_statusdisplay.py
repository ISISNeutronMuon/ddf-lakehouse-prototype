import argparse
import logging
from pathlib import Path
from typing import cast, Optional, Sequence

import dlt
from dlt import Pipeline
from dlt.common.configuration.resolve import inject_section
from dlt.common.configuration.specs import ConfigSectionContext
from dlt.common.libs.pandas import DataFrame
from dlt.common.pipeline import LoadInfo
from dlt.destinations.impl.filesystem.filesystem import FilesystemClient
from dlt.extract import DltSource
from dlt.sources.rest_api import rest_api_source
import humanize

from pipelines_common.pipeline import pipeline_name
import pipelines_common.utils.spark as spark_utils

# Runtime
LOGGER = logging.getLogger(__name__)
PIPELINE_BASENAME = "statusdisplay"
SPARK_APPNAME = Path(__file__).name

# Staging destination
LOADER_FILE_FORMAT = "parquet"


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


def create_pipeline(basename: str) -> Pipeline:
    """Given the base pipeline name return a new Pipeline object using the full qualified name"""
    pipeline_name_fq = pipeline_name(basename)
    return dlt.pipeline(
        pipeline_name=pipeline_name_fq,
        dataset_name=pipeline_name_fq,
        destination="filesystem",
        progress="log",
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


def find_latest_load_id(pipeline: Pipeline) -> Optional[str]:
    """Check the _dlt_loads table and retrieve the latest load_id"""
    with inject_section(ConfigSectionContext(pipeline_name=pipeline.pipeline_name)):
        ds = pipeline.dataset()
        _dlt_loads_df = cast(DataFrame, ds._dlt_loads.df())
        if len(_dlt_loads_df) > 0:
            return (
                _dlt_loads_df.sort_values("inserted_at", ascending=False)
                .head(1)["load_id"]
                .iat[0]
            )
        else:
            return None


def transform(pipeline: Pipeline, loads_ids: Sequence[str]):
    """Given the loads_ids for a loaded set of tables compute the final schedule table"""
    if pipeline.destination.destination_name != "filesystem":
        raise NotImplementedError(
            "Transforms are currently only implemented for the filesystem destination."
        )
    LOGGER.debug(f"Using load packages '{loads_ids}' as transform sources.")

    destination_client = cast(FilesystemClient, pipeline.destination_client())
    schedule_table_dir = destination_client.make_remote_url(
        destination_client.get_table_dir("schedule")
    )
    maintenance_table_dir = destination_client.make_remote_url(
        destination_client.get_table_dir("schedule__maintenance_days")
    )

    # Spark is not really required for this transformation given the data volume
    # but we use it for consistency across all imported sources and it's closer
    # to the data
    spark = spark_utils.create_spark_session(
        app_name=SPARK_APPNAME,
        controller_url=dlt.secrets["transform.spark.controller_url"],
        config=dlt.secrets["transform.spark.config"],
    )

    def read_parquet(path: str, load_id: str):
        return spark.read.option("recursiveFileLookup", "true").parquet(
            path, pathGlobFilter=f"{load_id}*.parquet"
        )

    def read_all_load_ids(path: str, loads_ids: Sequence[str]):
        all_df = None
        for load_id in loads_ids:
            df = read_parquet(path, load_id)
            if all_df is None:
                all_df = df
            else:
                all_df.unionAll(df)

        return all_df

    catalog_name, namespace_name = (
        dlt.config["transform.catalog_name"],
        dlt.config["transform.namespace_name"],
    )
    spark_utils.create_namespace_if_not_exists(spark, catalog_name, namespace_name)

    # Create merged table if required
    running_schedule_id = f"{catalog_name}.{namespace_name}.{dlt.config['transform.running_schedule_table_name']}"
    running_schedule_ddl = f"""CREATE TABLE IF NOT EXISTS {running_schedule_id} (
      type STRING,
      label STRING,
      start TIMESTAMP,
      end TIMESTAMP
    )
    USING iceberg
    """
    spark.sql(running_schedule_ddl)  # noqa

    # We currently only replace the whole thing everytime as the data is tiny...
    spark.sql(f"DELETE FROM {running_schedule_id}")

    raw_schedule_df = read_all_load_ids(schedule_table_dir, loads_ids)
    raw_maintenance_df = read_all_load_ids(maintenance_table_dir, loads_ids)
    # This is deliberately not an f-string. Instead it uses pyspark's parametrized sql call
    query = """
        (SELECT type, label, start, end
        FROM {raw_schedule_df})
        UNION ALL
        (SELECT 'maintenance' AS type, 'Maintenance Day' AS label,
          to_timestamp_ltz(split_part(value, "/", 1)) AS start,
          to_timestamp_ltz(split_part(value, "/", 2)) AS end
        FROM {raw_maintenance_df})
    """
    insert_into_df = spark.sql(
        query, raw_schedule_df=raw_schedule_df, raw_maintenance_df=raw_maintenance_df
    )
    insert_into_df.write.insertInto(running_schedule_id)


# ------------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--log-level", choices=logging.getLevelNamesMapping().keys())
    parser.add_argument(
        "--skip-extract-and-load",
        action="store_true",
        help="Skip the extract and load step.",
    )

    return parser.parse_args()


def configure_logging(root_level: int | str):
    class FilterUnwantedRecords:
        def filter(self, record):
            return record.name == "__main__"

    logging.basicConfig(level=root_level)
    for handler in logging.getLogger().handlers:
        handler.addFilter(FilterUnwantedRecords())


def main():
    args = parse_args()
    configure_logging(args.log_level)

    pipeline = create_pipeline(PIPELINE_BASENAME)
    LOGGER.info(f"-- Pipeline={pipeline.pipeline_name} --")

    if args.skip_extract_and_load:
        loads_ids = find_latest_load_id(pipeline)
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
