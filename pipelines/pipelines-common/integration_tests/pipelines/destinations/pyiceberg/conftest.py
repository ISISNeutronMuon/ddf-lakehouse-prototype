import os
from tempfile import TemporaryDirectory

import pytest

from integration_tests.pipelines.destinations.pyiceberg.utils import (
    PyIcebergDestinationTestConfiguration,
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
    yield destination_config
    destination_config.clean_catalog()
