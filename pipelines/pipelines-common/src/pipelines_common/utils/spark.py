from typing import Dict

from pyspark.sql import SparkSession


def create_spark_session(app_name: str, controller_url: str, config: Dict[str, str]):
    """Create Spark session using the current dlt.secrets values."""
    spark = (
        SparkSession.builder.master(controller_url)  # type: ignore
        .config(map=config)
        .appName(app_name)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
