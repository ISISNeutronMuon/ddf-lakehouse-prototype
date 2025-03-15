from pathlib import Path
from typing import cast, Dict, Sequence

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession

from .logging import logging

LOGGER = logging.getLogger(__name__)


def create_spark_session(
    app_name: str, controller_url: str, config: Dict[str, str], log_level: str = "WARN"
):
    """Create Spark session using the current dlt.secrets values.

    :param app_name: The name of the application
    :param controller_url: The url of the spark controller
    :param config: A dictionary of Spark config options
    :param log_level: The log level for the Spark session
    """
    spark = (
        SparkSession.builder.master(controller_url)  # type: ignore
        .config(map=config)
        .appName(app_name)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(log_level)

    return spark


def create_namespace_if_not_exists(
    spark: SparkSession, catalog_name: str, namespace_name: str
):
    """Create a given namespace in the catalog if it does not exist"""
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog_name}.{namespace_name}")


def read_parquet(spark: SparkSession, path: str, load_id: str) -> SparkDataFrame:
    """Read a set of parquet files found using the pathGlobFilter on the given path"""
    return spark.read.option("recursiveFileLookup", "true").parquet(
        path, pathGlobFilter=f"{load_id}*.parquet"
    )


def read_all_parquet(
    spark: SparkSession, path: str, loads_ids: Sequence[str]
) -> SparkDataFrame:
    all_df = None
    for load_id in loads_ids:
        df = read_parquet(spark, path, load_id)
        if all_df is None:
            all_df = df
        else:
            all_df.unionAll(df)

    return cast(SparkDataFrame, all_df)


def execute_sql_from_file(spark: SparkSession, filepath: Path) -> SparkDataFrame:
    """Read the SQL statements from the given file, execute and return the resultant DataFrame

    Multiple statements are separate by a semi-colon and executed in order. The final statement can
    contain a query that returns a DataFrame and this is returned to the caller.
    """
    with open(filepath) as fp:
        sql = fp.read()

    sql_stmts = sql.split(";")
    for stmt in sql_stmts[:-1]:
        LOGGER.debug(stmt)
        spark.sql(stmt)

    LOGGER.debug(sql_stmts[-1])
    return spark.sql(sql_stmts[-1])
