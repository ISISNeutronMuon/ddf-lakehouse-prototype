import inspect
from pathlib import Path
import shutil
from types import FrameType
from typing import Any, Dict, List

import dlt
from dlt.common.schema.utils import loads_table, pipeline_state_table

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
    pipeline.run(data_items(data), write_disposition="append")

    assert_table_has_shape(
        pipeline,
        f"{pipeline.dataset_name}._dlt_loads",
        expected_row_count=1,
        expected_schema=loads_table(),
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
    pipeline.run(data_items(data), write_disposition="replace")
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


# def test_drop_state(
#     pipelines_dir,
#     destination_config: PyIcebergDestinationTestConfiguration,
# ):
#     data = [{"id": i + 1} for i in range(2)]
#     pipeline = destination_config.setup_pipeline(
#         pipeline_name(inspect.currentframe()),
#         pipelines_dir=pipelines_dir,
#     )
#     # run a pipeline to populate the local & destination state and then remove local
#     pipeline.run(data_items(data))

#     with iceberg_catalog(pipeline) as catalog:
#         assert catalog.table_exists((pipeline.dataset_name, "_dlt_pipeline_state"))
