from typing import Iterator

import dlt
from dlt.common.configuration.exceptions import ConfigFieldMissingException
from dlt.pipeline.exceptions import PipelineStepFailed

from pipelines_common.dlt_sources.m365 import sharepoint, MsalCredentialsResource
from pipelines_common.m365.drive import DriveFileItem

from requests_mock.mocker import Mocker as RequestsMocker
import pytest
from pytest_mock import MockerFixture

from unit_tests.m365.conftest import patch_msal_with_access_token


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


def test_load_from_sharepoint(
    pipeline: dlt.Pipeline, mocker: MockerFixture, requests_mock: RequestsMocker
):
    site_url, folder, file_name, file_content = (
        "https://name.host.com/sites/TestSite",
        "Folder",
        "TestFile.csv",
        b"colA,colb\n1,2",
    )
    file_path = f"{folder}/{file_name}"

    @dlt.transformer
    def assert_expected_drive_items(items: Iterator[DriveFileItem]):
        drive_items = list(items)
        assert len(drive_items) == 1

        assert isinstance(drive_items[0], DriveFileItem)
        assert drive_items[0].name == file_name
        assert drive_items[0].content == file_content

    patched_msal_client_app = patch_msal_with_access_token(mocker)
    requests_mock.get(
        "https://graph.microsoft.com/v1.0/sites/name.host.com:/sites/TestSite",
        json={"id": "TestSiteID123"},
    )
    requests_mock.get(
        "https://graph.microsoft.com/v1.0/sites/TestSiteID123/drive",
        json={"id": "TestDriveID123"},
    )
    requests_mock.get(
        "https://graph.microsoft.com/v1.0/drives/TestDriveID123/root",
        json={"id": "TestDriveRootID123", "name": "root"},
    )
    requests_mock.get(
        f"https://graph.microsoft.com/v1.0/drives/TestDriveID123/root:/{file_path}",
        json={
            "id": "TestDriveItemID123",
            "name": file_name,
            "@microsoft.graph.downloadUrl": "https://file.download.url/458972349iuf",
        },
    )
    requests_mock.get("https://file.download.url/458972349iuf", content=file_content)

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
