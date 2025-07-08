from tempfile import TemporaryDirectory

import dlt

import pytest


@pytest.fixture
def pipeline():
    with TemporaryDirectory() as tmp_dir:
        pipeline = dlt.pipeline(
            pipeline_name="test_sharepoint_source_pipeline",
            pipelines_dir=tmp_dir,
            destination=dlt.destinations.duckdb(f"{tmp_dir}/data.db"),
            dataset_name="sharepoint_data",
            dev_mode=True,
        )
        yield pipeline
