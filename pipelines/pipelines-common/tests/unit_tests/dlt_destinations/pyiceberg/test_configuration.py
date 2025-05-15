from typing import Any

from dlt.common.configuration import resolve

from pipelines_common.dlt_destinations.pyiceberg.configuration import (
    IcebergClientConfiguration,
)


def test_initial_iceberg_config_state_defaults_to_rest():
    default_config = IcebergClientConfiguration()

    assert default_config.bucket_url is None
    assert default_config.catalog_type == "rest"
    assert default_config.table_location_layout == r"{dataset_name}/{table_name}"
    assert default_config.credentials is None
    assert default_config.connection_properties == {}


def test_bucket_url_trimmed_of_trailing_slash_when_resolved():
    config = resolve.resolve_configuration(
        IcebergClientConfiguration(),
        explicit_value={
            "bucket_url": "s3://mybucket/",
            "dataset_name": "mydataset",
            "credentials": {
                "uri": "http://catalog_host/catalog",
            },
        },
    )

    assert config.bucket_url == "s3://mybucket"
