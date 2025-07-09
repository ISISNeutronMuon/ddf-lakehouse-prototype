from pathlib import Path
from typing import Iterator

import dlt
from dlt.common.configuration.exceptions import ConfigFieldMissingException
from dlt.pipeline.exceptions import PipelineStepFailed

from pipelines_common.dlt_sources.m365 import sharepoint, MsalCredentialsResource
from pipelines_common.m365.drive import DriveFileItem

from requests_mock.mocker import Mocker as RequestsMocker
import pytest
from pytest_mock import MockerFixture

from unit_tests.m365.conftest import patch_msal_with_access_token, SharePointTestSettings


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
    for field in ("site_url", "file_glob"):
        assert field in config_exc.fields


def test_extract_single_file_from_sharepoint(
    pipeline: dlt.Pipeline, mocker: MockerFixture, requests_mock: RequestsMocker
):
    site_url = f"https://{SharePointTestSettings.HOSTNAME}/{SharePointTestSettings.SITE_PATH}"
    folder, file_name, file_content = (
        "Folder",
        "TestFile.csv",
        b"colA,colb\n1,2",
    )
    file_path = f"{folder}/{file_name}"
    patched_msal_client_app = patch_msal_with_access_token(mocker)
    SharePointTestSettings.mock_requests_for_file(requests_mock, file_path, file_content)

    @dlt.transformer
    def assert_expected_drive_items(items: Iterator[DriveFileItem]):
        drive_items = list(items)
        assert len(drive_items) == 1

        assert isinstance(drive_items[0], DriveFileItem)
        assert drive_items[0].name == file_name
        assert drive_items[0].content == file_content

    credentials = MsalCredentialsResource("tid", "cid", "cs3cr3t")
    data = (
        sharepoint(site_url, credentials=credentials, file_glob=file_path)
        | assert_expected_drive_items
    )
    pipeline.extract(data())

    assert patched_msal_client_app.call_count == 4
    call_kwargs = patched_msal_client_app.call_args[1]
    assert credentials.tenant_id in call_kwargs["authority"]
    assert credentials.client_id == call_kwargs["client_id"]
    assert credentials.client_secret == call_kwargs["client_credential"]


def test_extract_files_with_glob(
    pipeline: dlt.Pipeline, mocker: MockerFixture, requests_mock: RequestsMocker
):
    site_url = f"https://{SharePointTestSettings.HOSTNAME}/{SharePointTestSettings.SITE_PATH}"
    file_glob = "outer/prefix-*/*.csv"
    file_paths = [
        f"outer/prefix-{dir_num}/{file_num}.csv" for dir_num in range(3) for file_num in range(2)
    ]

    patched_msal_client_app = patch_msal_with_access_token(mocker)
    for file_path in file_paths:
        SharePointTestSettings.mock_requests_for_file(requests_mock, file_path, file_path.encode())

    @dlt.transformer
    def assert_expected_drive_items(items: Iterator[DriveFileItem]):
        drive_items = list(items)
        assert len(drive_items) == len(file_paths)

        for file_path, drive_item in zip(file_paths, drive_items):
            assert isinstance(drive_items, DriveFileItem)
            assert drive_item.name == Path(file_path).name
            assert drive_item.content == file_path

    credentials = MsalCredentialsResource("tid", "cid", "cs3cr3t")
    data = (
        sharepoint(site_url, credentials=credentials, file_glob=file_glob)
        | assert_expected_drive_items
    )
    pipeline.extract(data())

    patched_msal_client_app.assert_called()
