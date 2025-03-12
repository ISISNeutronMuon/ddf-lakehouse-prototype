from typing import Dict

from pyspark.sql import SparkSession


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
