import datetime
import re
from typing import Iterator

import dlt
from dlt.common.configuration.exceptions import ConfigFieldMissingException
from dlt.common.storages.fsspec_filesystem import FileItemDict
from dlt.pipeline.exceptions import PipelineStepFailed

from pipelines_common.dlt_sources.m365 import sharepoint, M365CredentialsResource

import pytest
from pytest_mock import MockerFixture
from pytest_httpx import HTTPXMock

from unit_tests.dlt_sources.conftest import SharePointTestSettings


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
        pipeline.extract(sharepoint(credentials=M365CredentialsResource("tid", "cid", "cs3cr3t")))

    config_exc = exc.value.exception
    assert isinstance(config_exc, ConfigFieldMissingException)
    for field in ("site_url", "file_glob"):
        assert field in config_exc.fields


@pytest.mark.parametrize("extract_content", (False,))
def test_extract_sharepoint_files_yields_files_matching_glob(
    pipeline: dlt.Pipeline, mocker: MockerFixture, httpx_mock: HTTPXMock, extract_content: bool
):
    @dlt.transformer
    def assert_expected_drive_items(
        drive_items: Iterator[FileItemDict], expected_paths: Iterator[str]
    ):
        for drive_obj, expected_path in zip(drive_items, expected_paths):
            assert drive_obj["file_url"] == f"msgd://{expected_path}"
            assert extract_content == ("file_content" in drive_obj)

    site_url = f"https://{SharePointTestSettings.HOSTNAME}/{SharePointTestSettings.SITE_PATH}"
    credentials = M365CredentialsResource("tid", "cid", "cs3cr3t")
    # mock the calls to return the token & drive_id
    httpx_mock.add_response(
        method="POST",
        url=credentials.oauth2_token_endpoint,
        json={"access_token": SharePointTestSettings.ACCESS_TOKEN},
    )
    httpx_mock.add_response(
        method="GET",
        url=re.compile(
            f"{M365CredentialsResource.api_url}/sites/{SharePointTestSettings.HOSTNAME}:/{SharePointTestSettings.SITE_PATH}?.*"
        ),
        json={"id": SharePointTestSettings.SITE_ID},
    )
    httpx_mock.add_response(
        method="GET",
        url=re.compile(
            f"{M365CredentialsResource.api_url}/sites/{SharePointTestSettings.SITE_ID}/drive?.*"
        ),
        json={"id": SharePointTestSettings.LIBRARY_ID},
    )

    # Mock out drive client to return known items from glob.
    # Beware that the test_glob & resulting return values are not linked so changing test_glob
    # will not affect the glob.return_value. Here we are relying on the MSDDriveFS client to do the
    # right thing and we just test we call it correctly.
    patched_drivefs_cls = mocker.patch("pipelines_common.dlt_sources.m365.MSGDriveFS")
    patched_drivefs = patched_drivefs_cls.return_value
    test_glob = "/Folder/*.csv"
    patched_drivefs.glob.return_value = {
        "/Folder/0.csv": {
            "name": "/Folder/0.csv",
            "type": "file",
            "mtime": datetime.datetime.fromisoformat("1970-01-01T00:00:00Z"),
            "size": "0",
        }
    }

    expected_glob_paths = patched_drivefs.glob.return_value.keys()
    pipeline.extract(
        sharepoint(site_url, credentials=credentials, file_glob=test_glob)
        | assert_expected_drive_items(expected_glob_paths)
    )

    patched_drivefs_cls.assert_called_once_with(
        drive_id=SharePointTestSettings.LIBRARY_ID,
        oauth2_client_params=credentials.oauth2_client_params(
            {"access_token": SharePointTestSettings.ACCESS_TOKEN}
        ),
    )
    patched_drivefs.glob.assert_called_with(test_glob, detail=True)
