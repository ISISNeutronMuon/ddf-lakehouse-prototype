from integration_tests.pipelines.utils import DestinationIcebergTestConfiguration

import pytest


@pytest.fixture
def iceberg_test_config() -> DestinationIcebergTestConfiguration:
    destination_config = DestinationIcebergTestConfiguration(
        destination="pipelines_common.dlt_destinations.iceberg"
    )

    return destination_config


def test_iceberg_no_partitions(
    iceberg_test_config: DestinationIcebergTestConfiguration,
) -> None:
    pipeline = iceberg_test_config.setup_pipeline("test_iceberg_no_partitions")
    pytest.fail("Implement the test!")
