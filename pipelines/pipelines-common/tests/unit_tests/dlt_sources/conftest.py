from tempfile import TemporaryDirectory

import dlt
from dlt.common.pipeline import LoadInfo
from dlt.common.typing import DictStrAny

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


def assert_load_info(info: LoadInfo, expected_load_packages: int = 1) -> None:
    """Asserts that expected number of packages was loaded and there are no failed jobs"""
    assert len(info.loads_ids) == expected_load_packages
    # all packages loaded
    assert all(package.state == "loaded" for package in info.load_packages) is True
    # Explicitly check for no failed job in any load package. In case a terminal exception was disabled by raise_on_failed_jobs=False
    info.raise_on_failed_jobs()


def load_table_counts(p: dlt.Pipeline, *table_names: str) -> DictStrAny:
    """Returns row counts for `table_names` as dict"""
    with p.sql_client() as c:
        query = "\nUNION ALL\n".join(
            [
                f"SELECT '{name}' as name, COUNT(1) as c FROM {c.make_qualified_table_name(name)}"
                for name in table_names
            ]
        )
        with c.execute_query(query) as cur:
            rows = list(cur.fetchall())
            return {r[0]: r[1] for r in rows}
