from typing import Iterator

import dlt
from dlt.common.configuration.exceptions import ConfigFieldMissingException
from dlt.pipeline.exceptions import PipelineStepFailed

from pipelines_common.dlt_sources.m365 import sharepoint, MsalCredentialsResource
from pipelines_common.m365.drive import DriveItem
import pytest

from unit_tests.dlt_sources.conftest import assert_load_info, load_table_counts


def test_extract_sharepoint_source_raises_error_without_config(pipeline: dlt.Pipeline):
    # first without credentials
    with pytest.raises(PipelineStepFailed) as exc:
        pipeline.extract(sharepoint())

    config_exc = exc.value.exception
    assert isinstance(config_exc, ConfigFieldMissingException)
    for field in ("tenant_id", "client_id", "client_secret"):
        assert field in config_exc.fields

    # then other config
    with pytest.raises(PipelineStepFailed) as exc:
        pipeline.extract(sharepoint(credentials=MsalCredentialsResource("tid", "cid", "cs3cr3t")))

    config_exc = exc.value.exception
    assert isinstance(config_exc, ConfigFieldMissingException)
    for field in ("hostname", "site_path", "file_path"):
        assert field in config_exc.fields


def test_load_from_sharepoint(pipeline: dlt.Pipeline):
    hostname, site_path, file_path, file_content = (
        "name.host.com",
        "sites/TestSite",
        "Folder/TestFile.csv",
        b"colA,colb\n1,2",
    )

    @dlt.transformer
    def assert_expected_drive_items(items: Iterator[DriveItem]):
        drive_items = list(items)
        assert len(drive_items) == 1

        assert drive_items[0] == hostname

    credentials = MsalCredentialsResource("tid", "cid", "cs3cr3t")

    data = (
        sharepoint(hostname, site_path, credentials=credentials, file_glob=file_path)
        | assert_expected_drive_items
    )
    pipeline.extract(data())
