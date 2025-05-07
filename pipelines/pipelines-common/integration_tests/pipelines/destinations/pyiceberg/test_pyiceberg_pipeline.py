import inspect
import shutil
from types import FrameType
from typing import Any, Dict, List

import dlt
from dlt.common.pendulum import pendulum
from dlt.common.schema.utils import loads_table, pipeline_state_table, version_table

from pipelines_common.dlt_destinations.pyiceberg.catalog import (
    namespace_exists as catalog_namespace_exists,
)


from integration_tests.pipelines.destinations.pyiceberg.utils import (
    assert_table_has_shape,
    assert_table_has_data,
    iceberg_catalog,
    PyIcebergDestinationTestConfiguration,
)


def pipeline_name(frame: FrameType | None):
    # Use the function name
    return frame.f_code.co_name if frame is not None else "pipeline_name_frame_none"


@dlt.resource()
def data_items(data: List[Dict[str, Any]] | None = None):
    data = [] if data is None else data
    yield data


def test_dlt_tables_created(
    pipelines_dir,
    destination_config: PyIcebergDestinationTestConfiguration,
) -> None:
    data = [{"id": 1}]
    pipeline = destination_config.setup_pipeline(
        pipeline_name(inspect.currentframe()),
        pipelines_dir=pipelines_dir,
    )
    pipeline.run(data_items(data))

    assert_table_has_shape(
        pipeline,
        f"{pipeline.dataset_name}._dlt_loads",
        expected_row_count=1,
        expected_schema=loads_table(),
    )
    assert_table_has_shape(
        pipeline,
        f"{pipeline.dataset_name}._dlt_version",
        expected_row_count=1,
        expected_schema=version_table(),
    )
    assert_table_has_shape(
        pipeline,
        f"{pipeline.dataset_name}._dlt_pipeline_state",
        expected_row_count=1,
        expected_schema=pipeline_state_table(),
    )


def test_explicit_append(
    pipelines_dir,
    destination_config: PyIcebergDestinationTestConfiguration,
) -> None:
    data = [
        {
            "id": i + 1,
            "category": "A",
        }
        for i in range(10)
    ]
    pipeline = destination_config.setup_pipeline(
        pipeline_name(inspect.currentframe()),
        pipelines_dir=pipelines_dir,
    )
    # first run
    pipeline.run(data_items(data), write_disposition="append")
    assert_table_has_data(
        pipeline,
        f"{pipeline.dataset_name}.data_items",
        expected_items_count=len(data),
        items=data,
    )

    # run again and see we get duplicated records
    data_second = [
        {
            "id": 11 + i,
            "category": "B",
        }
        for i in range(10)
    ]
    pipeline.run(data_items(data_second), write_disposition="append")
    final_data = data
    final_data.extend(data_second)
    assert_table_has_data(
        pipeline,
        f"{pipeline.dataset_name}.data_items",
        expected_items_count=len(final_data),
        items=final_data,
    )


def test_explicit_replace(
    pipelines_dir,
    destination_config: PyIcebergDestinationTestConfiguration,
) -> None:
    data = [
        {
            "id": i + 1,
            "category": "A",
        }
        for i in range(10)
    ]
    pipeline = destination_config.setup_pipeline(
        pipeline_name(inspect.currentframe()),
        pipelines_dir=pipelines_dir,
    )
    # first run
    pipeline.run(data_items(data))
    assert_table_has_data(
        pipeline,
        f"{pipeline.dataset_name}.data_items",
        expected_items_count=len(data),
        items=data,
    )

    # run again and see we a single copy of the records from the second dataset
    data_second = [
        {
            "id": 11 + i,
            "category": "B",
        }
        for i in range(10)
    ]
    pipeline.run(data_items(data_second), write_disposition="replace")
    assert_table_has_data(
        pipeline,
        f"{pipeline.dataset_name}.data_items",
        expected_items_count=len(data_second),
        items=data_second,
    )


def test_drop_storage(
    pipelines_dir,
    destination_config: PyIcebergDestinationTestConfiguration,
):
    data = [{"id": i + 1} for i in range(2)]
    pipeline = destination_config.setup_pipeline(
        pipeline_name(inspect.currentframe()),
        pipelines_dir=pipelines_dir,
    )
    # run a pipeline to populate destination state and then drop the storage
    pipeline.run(data_items(data))

    with pipeline.destination_client() as client:
        client.drop_storage()

    with iceberg_catalog(pipeline) as catalog:
        assert not catalog_namespace_exists(catalog, pipeline.dataset_name)


def test_sync_state(
    pipelines_dir, destination_config: PyIcebergDestinationTestConfiguration
):
    data = [{"id": i + 1} for i in range(2)]
    pipeline_1 = destination_config.setup_pipeline(
        pipeline_name(inspect.currentframe()),
        pipelines_dir=pipelines_dir,
    )
    # run a pipeline to populate destination state, remove local state and run again
    pipeline_1.run(data_items(data))
    shutil.rmtree(pipelines_dir)
    pipeline_2 = destination_config.setup_pipeline(
        pipeline_name(inspect.currentframe()),
        pipelines_dir=pipelines_dir,
    )
    pipeline_2.run(data_items(data), write_disposition="replace")

    assert pipeline_2.state == pipeline_1.state


def test_expected_datatypes_can_be_loaded(
    pipelines_dir, destination_config: PyIcebergDestinationTestConfiguration
):
    data = [
        {
            "integer": 1,
            "text": "text value",
            "boolean": True,
            "timestamp": pendulum.datetime(2025, 5, 7, 14, 29, 31),
            "date": pendulum.date(2025, 5, 7),
        }
    ]
    pipeline = destination_config.setup_pipeline(
        pipeline_name(inspect.currentframe()),
        pipelines_dir=pipelines_dir,
    )
    pipeline.run(data_items(data))

    assert_table_has_data(
        pipeline,
        f"{pipeline.dataset_name}.data_items",
        expected_items_count=len(data),
        items=data,
    )
