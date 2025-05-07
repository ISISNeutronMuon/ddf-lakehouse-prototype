import os
from tempfile import TemporaryDirectory

import pytest
from pipelines_common.dlt_destinations.pyiceberg.catalog import (
    create_catalog as create_iceberg_catalog,
)

from integration_tests.pipelines.destinations.pyiceberg.utils import (
    PyIcebergDestinationTestConfiguration,
)


@pytest.fixture(autouse=True)
def ensure_iceberg_catalog_exists():
    # Pre-flight check - is the iceberg catalog accessible?
    import requests.exceptions

    try:
        create_iceberg_catalog(
            "pytest_configure",
            uri=os.environ["DESTINATION__PYICEBERG__CREDENTIALS__URI"],
            warehouse=os.environ["DESTINATION__PYICEBERG__CREDENTIALS__WAREHOUSE"],
        )
    except requests.exceptions.ConnectionError as exc:
        pytest.fail(
            f"Failed pre-flight checks. Iceberg catalog not reachable: {str(exc)}"
        )


@pytest.fixture
def pipelines_dir():
    with TemporaryDirectory() as tmp_dir:
        yield tmp_dir


@pytest.fixture
def destination_config():
    destination_config = PyIcebergDestinationTestConfiguration(
        credentials={
            "uri": os.environ["DESTINATION__PYICEBERG__CREDENTIALS__URI"],
            "warehouse": os.environ["DESTINATION__PYICEBERG__CREDENTIALS__WAREHOUSE"],
        },
        env_vars=({"DESTINATION__PYICEBERG__CATALOG_TYPE": "rest"}),
    )
    try:
        yield destination_config
    finally:
        destination_config.clean_catalog()
