from typing import cast, Any, List

import dlt
from dlt.common.schema.utils import loads_table
import pyarrow as pa
import pytest
from pipelines_common.dlt_destinations.pyiceberg.pyiceberg import PyIcebergClient

from integration_tests.pipelines.destinations.pyiceberg.utils import (
    PyIcebergDestinationTestConfiguration,
)


def test_explicit_append(
    pipelines_dir,
    destination_config: PyIcebergDestinationTestConfiguration,
) -> None:
    data = [{"id": i + 1, "category": "A" if i <= 5 else "B"} for i in range(10)]

    @dlt.resource()
    def data_items():
        yield data

    pipeline_name = "test_explicit_append"
    pipeline = destination_config.setup_pipeline(
        pipeline_name,
        pipelines_dir=pipelines_dir,
    )
    pipeline.run(data_items)

    assert_table(
        pipeline,
        f"{pipeline.dataset_name}.data_items",
        expected_items_count=len(data),
        items=data,
    )


def assert_table(
    pipeline: dlt.Pipeline,
    qualified_table_name: str,
    *,
    expected_items_count: int,
    items: List[Any] = None,
):
    with pipeline.destination_client() as client:
        catalog = cast(PyIcebergClient, client).iceberg_catalog
        assert catalog.table_exists(qualified_table_name)

        table = catalog.load_table(qualified_table_name)

    assert table.scan().count() == expected_items_count

    if items is None:
        return

    arrow_table = table.scan().to_arrow()
    drop_keys = ["_dlt_id", "_dlt_load_id"]
    objects_without_dlt_keys = [
        {k: v for k, v in record.items() if k not in drop_keys}
        for record in arrow_table.to_pylist()
    ]

    assert_unordered_list_equal(objects_without_dlt_keys, items)


def assert_unordered_list_equal(list1: List[Any], list2: List[Any]) -> None:
    assert len(list1) == len(list2), "Lists have different length"
    for item in list1:
        assert item in list2, f"Item {item} not found in list2"
