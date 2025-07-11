import datetime
import itertools
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


@pytest.mark.parametrize(
    ["files_per_page", "extract_content"],
    [
        (100, False),
        #        (100, True),
        (2, False),
    ],
)
def test_extract_sharepoint_files_yields_files_matching_glob(
    pipeline: dlt.Pipeline,
    mocker: MockerFixture,
    httpx_mock: HTTPXMock,
    files_per_page: int,
    extract_content: bool,
):
    num_files_found = 6
    drivefs_glob_return_value = {
        f"/Folder/{index}.csv": {
            "name": f"/Folder/{index}.csv",
            "type": "file",
            "mtime": datetime.datetime.fromisoformat("1970-01-01T00:00:00Z"),
            "size": "0",
        }
        for index in range(num_files_found)
    }
    transformer_calls = 0
    if files_per_page >= num_files_found:
        expected_transfomer_calls = 1
        expected_transfomer_item_sizes = [num_files_found]
    else:
        expected_transfomer_calls = (
            int(num_files_found / files_per_page) + num_files_found % files_per_page
        )
        expected_transfomer_item_sizes = [
            sum(batch) for batch in itertools.batched([1] * num_files_found, files_per_page)
        ]

    @dlt.transformer
    def assert_expected_drive_items(drive_items: Iterator[FileItemDict]):
        nonlocal transformer_calls
        assert len(list(drive_items)) == expected_transfomer_item_sizes[transformer_calls]
        # for drive_obj, expected_path in zip(drive_items, expected_paths):
        #     assert drive_obj["file_url"] == f"msgd://{expected_path}"
        #     assert extract_content == ("file_content" in drive_obj)
        transformer_calls += 1

    credentials = M365CredentialsResource("tid", "cid", "cs3cr3t")
    SharePointTestSettings.mock_get_drive_id_responses(httpx_mock, credentials)

    # Mock out drive client to return known items from glob.
    # Beware that the test_glob & resulting return values are not linked so changing test_glob
    # will not affect the glob.return_value. Here we are relying on the MSDDriveFS client to do the
    # right thing and we just test we call it correctly.
    patched_drivefs_cls = mocker.patch("pipelines_common.dlt_sources.m365.MSGDriveFS")
    patched_drivefs = patched_drivefs_cls.return_value
    test_glob = "/Folder/*.csv"
    patched_drivefs.glob.return_value = drivefs_glob_return_value

    pipeline.extract(
        sharepoint(
            SharePointTestSettings.site_url,
            credentials=credentials,
            file_glob=test_glob,
            files_per_page=files_per_page,
        )
        | assert_expected_drive_items()
    )

    patched_drivefs_cls.assert_called_once_with(
        drive_id=SharePointTestSettings.library_id,
        oauth2_client_params=credentials.oauth2_client_params(
            {"access_token": SharePointTestSettings.access_token}
        ),
    )
    patched_drivefs.glob.assert_called_with(test_glob, detail=True)
    assert expected_transfomer_calls == transformer_calls
