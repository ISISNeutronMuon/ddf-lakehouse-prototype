from collections.abc import Generator
import glob
import os
from pathlib import Path

import dlt
from dlt import Pipeline
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
PIPELINE_BASENAME = "opralog_desktop"
DATASET_NAME = "opralogdb"
SPARK_APPNAME = Path(__file__).name

# Transforms
MODELS_DIR = Path(__file__).parent / "models"
SCHEMAS_DIR = Path(__file__).parent / "schemas"


def transform(pipeline: Pipeline):
    """Create/update models based on the loaded data."""
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


class Model:

    def __init__(
        self,
        catalog_name: str,
        namespace_name: str,
        table_name: str,
        schema_path: Path,
        model_path: Path,
    ):
        """Encapsulate a model definition and associated schema.

        This current only handles a single schema.

        :param catalog_name: Name of the destination catalog
        :param namespace_name: Name of the destination namespace
        :param table_name: Name of the model table
        :param schema_path: Path to the schema definition
        :param model_path: Path to model computation script
        """
        self._catalog_name = catalog_name
        self._namespace_name = namespace_name
        self._table_name = table_name
        self._schema_path = schema_path
        self._model_path = model_path

    @property
    def catalog_name(self) -> str:
        return self._catalog_name

    @property
    def namespace_name(self) -> str:
        return self._namespace_name

    @property
    def model_path(self) -> Path:
        return self._model_path

    @property
    def schema_path(self) -> Path:
        return self._schema_path

    @property
    def table_name(self) -> str:
        return self._table_name


def models() -> Generator[Model]:
    for model_script in glob.glob("**/*.py", root_dir=MODELS_DIR, recursive=True):
        catalog_name, namespace_name, table_name = model_script.split(os.sep)[:-1]
        model_path = MODELS_DIR / model_script
        schema_path = (
            SCHEMAS_DIR / catalog_name / namespace_name / table_name / "create.sql"
        )
        yield Model(catalog_name, namespace_name, table_name, schema_path, model_path)


def main():
    args = cli_utils.create_common_argparser().parse_args()
    logging_utils.configure_logging(args.log_level, keep_records_from=["__main__"])

    pipeline = pipeline_utils.create_pipeline(
        PIPELINE_BASENAME,
        destination="filesystem",
        dataset_name=DATASET_NAME,
        progress="log",
    )
    LOGGER.info(f"----- Transform -----")
    LOGGER.info(f"Pipeline: {pipeline.pipeline_name}, Dataset {pipeline.dataset_name}")

    # transform(pipeline)
    for m in models():
        print(m)


if __name__ == "__main__":
    main()
