import io
from pathlib import Path
from typing import Iterator

import dlt
from dlt.common.configuration.exceptions import ConfigFieldMissingException
from dlt.pipeline.exceptions import PipelineStepFailed

from pipelines_common.dlt_sources.m365 import sharepoint, M365CredentialsResource
from pipelines_common.m365.sharepoint import MSGDriveFS


import pytest
from pytest_mock import MockerFixture
from pytest_httpx import HTTPXMock

from unit_tests.m365.conftest import (
    MSGraphTestSettings,
    DriveGlobTestCase,
    SharePointTestSettings,
    patch_oauth_client_to_return,
)


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


# @pytest.mark.httpx_mock(assert_all_responses_were_requested=False)
@pytest.mark.parametrize(
    "glob_test_case",
    SharePointTestSettings.drive_glob_test_cases(),
    ids=lambda x: x.name,
)
def test_extract_sharepoint_files_matching_returns_expected_files(
    pipeline: dlt.Pipeline,
    mocker: MockerFixture,
    httpx_mock: HTTPXMock,
    glob_test_case: DriveGlobTestCase,
):
    from msgraphfs import MSGraphBuffredFile

    @dlt.transformer
    def assert_expected_drive_items(items):
        drive_items = list(items)
        assert len(drive_items) == 1

        assert isinstance(drive_items[0], MSGraphBuffredFile)
        assert drive_items[0].path == glob_test_case.expected_matching_files
        # assert drive_items[0].content == file_content

        # expected_file_paths = glob_test_cases.mock_requests_for_driveitem_children(
        #     requests_mock, drive.drive_id
        # )

    patched_oauth_client = patch_oauth_client_to_return(
        {"access_token": MSGraphTestSettings.ACCESS_TOKEN}, mocker
    )
    SharePointTestSettings.mock_requests_to_library(httpx_mock)
    glob_test_case.mock_requests_for_driveitem_children(httpx_mock)

    site_url = f"https://{SharePointTestSettings.HOSTNAME}/{SharePointTestSettings.SITE_PATH}"
    credentials = M365CredentialsResource("tid", "cid", "cs3cr3t")
    pipeline.extract(
        sharepoint(site_url, credentials=credentials, file_glob=glob_test_case.glob_pattern)
    )

    patched_oauth_client.assert_called()


# # def test_extract_files_with_glob(
# #     pipeline: dlt.Pipeline, mocker: MockerFixture, requests_mock: RequestsMocker
# # ):
# #     site_url = f"https://{SharePointTestSettings.HOSTNAME}/{SharePointTestSettings.SITE_PATH}"
# #     file_glob = "outer/prefix-*/*.csv"
# #     file_paths = [
# #         f"outer/prefix-{dir_num}/{file_num}.csv" for dir_num in range(3) for file_num in range(2)
# #     ]

# #     patched_msal_client_app = patch_msal_with_access_token(mocker)
# #     for file_path in file_paths:
# #         SharePointTestSettings.mock_requests_for_file(requests_mock, file_path, file_path.encode())

# #     @dlt.transformer
# #     def assert_expected_drive_items(items: Iterator[DriveFileItem]):
# #         drive_items = list(items)
# #         assert len(drive_items) == len(file_paths)

# #         for file_path, drive_item in zip(file_paths, drive_items):
# #             assert isinstance(drive_items, DriveFileItem)
# #             assert drive_item.name == Path(file_path).name
# #             assert drive_item.content == file_path

# #     credentials = MsalCredentialsResource("tid", "cid", "cs3cr3t")
# #     data = (
# #         sharepoint(site_url, credentials=credentials, file_glob=file_glob)
# #         | assert_expected_drive_items
# #     )
# #     pipeline.extract(data())

# #     patched_msal_client_app.assert_called()
