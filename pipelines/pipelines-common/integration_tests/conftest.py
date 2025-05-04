import os
from typing import List

# patch which providers to enable
from dlt.common.configuration.providers import (
    ConfigProvider,
    EnvironProvider,
    SecretsTomlProvider,
    ConfigTomlProvider,
)
from dlt.common.runtime.run_context import RunContext
import pytest

from pipelines_common.dlt_destinations.pyiceberg.catalog import (
    create_catalog as create_iceberg_catalog,
)


def initial_providers(self) -> List[ConfigProvider]:
    # do not read the global config
    # find the .dlt in the same directory as this file
    return [
        EnvironProvider(),
        SecretsTomlProvider(settings_dir="tests/.dlt"),
        ConfigTomlProvider(settings_dir="tests/.dlt"),
    ]


RunContext.initial_providers = initial_providers  # type: ignore[method-assign]


def pytest_configure(config):
    # Iceberg catalog details - default to match settings in local docker-compose setup.
    os.environ.setdefault(
        "DESTINATION__PYICEBERG__CREDENTIALS__URI", "http://localhost:8181/catalog"
    )
    os.environ.setdefault("DESTINATION__PYICEBERG__CREDENTIALS__WAREHOUSE", "demo")

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
