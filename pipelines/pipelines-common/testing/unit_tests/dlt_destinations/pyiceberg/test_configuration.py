from pipelines_common.dlt_destinations.pyiceberg.configuration import (
    IcebergClientConfiguration,
)


def test_initial_iceberg_config_state():
    default_config = IcebergClientConfiguration()

    assert default_config.catalog_type == "rest"
    assert default_config.table_location_layout == r"{dataset_name}/{table_name}"
    assert default_config.credentials is None
    assert default_config.connection_properties == {}
