import dataclasses
import re
from unittest.mock import MagicMock

import requests

from pipelines_common.sharepoint.msgraph import (
    MSGraphV1,
)
from pipelines_common.sharepoint.sharepoint import SharePointSite

import pytest
from pytest_mock import MockerFixture
from requests_mock.mocker import Mocker as RequestsMocker

from unit_tests.sharepoint.conftest import SharePointTestSettings


@dataclasses.dataclass
class PatchedSharePointSite:
    instance: SharePointSite
    patched_requests_get: MagicMock
    patched_requests_response: MagicMock


@pytest.fixture
def patched_sp_site(
    graph_client_with_access_token: MSGraphV1, mocker: MockerFixture
) -> PatchedSharePointSite:
    patched_requests_response = mocker.patch(
        "pipelines_common.sharepoint.sharepoint.requests.Response"
    )
    patched_requests_response.json.return_value = {"id": SharePointTestSettings.SITE_ID}
    patched_requests_get = mocker.patch("pipelines_common.sharepoint.sharepoint.requests.get")
    patched_requests_get.return_value = patched_requests_response

    sp_site = SharePointSite(
        graph_client_with_access_token,
        SharePointTestSettings.HOSTNAME,
        SharePointTestSettings.SITE_PATH,
    )

    return PatchedSharePointSite(sp_site, patched_requests_get, patched_requests_response)


@pytest.fixture
def sharepoint_site(
    graph_client_with_access_token: MSGraphV1, requests_mock: RequestsMocker
) -> SharePointSite:
    matcher = re.compile(
        f"{graph_client_with_access_token.api_url}/sites/{SharePointTestSettings.HOSTNAME}:/{SharePointTestSettings.SITE_PATH}"
    )
    requests_mock.get(
        matcher,
        json={"id": SharePointTestSettings.SITE_ID},
    )

    return SharePointSite(
        graph_client_with_access_token,
        SharePointTestSettings.HOSTNAME,
        SharePointTestSettings.SITE_PATH,
    )


def test_sharepointsite_init_raises_error_request_raises_error(
    graph_client_with_access_token: MSGraphV1, requests_mock: RequestsMocker
) -> None:
    requests_mock.get(re.compile(".*"), exc=requests.exceptions.InvalidURL)
    # patched_requests_get = mocker.patch("pipelines_common.sharepoint.sharepoint.requests.get")
    # patched_requests_get.side_effect = RuntimeError("requests error")

    with pytest.raises(requests.exceptions.InvalidURL):
        SharePointSite(
            graph_client_with_access_token,
            SharePointTestSettings.HOSTNAME,
            SharePointTestSettings.SITE_PATH,
        )


def test_sharepointsite_init_stores_id_of_site(
    graph_client_with_access_token, requests_mock
) -> None:
    sp_site = sharepoint_site(graph_client_with_access_token, requests_mock)

    assert sp_site.id == SharePointTestSettings.SITE_ID
    assert len(requests_mock.request_history) == 1
    assert (
        requests_mock.request_history[0].url
        == f"{sp_site.graph_client.api_url}/sites/{SharePointTestSettings.HOSTNAME}:/sites/MySite"
    )


def test_sharepoint_default_document_library_produces_drive_with_expected_id(
    patched_sp_site: PatchedSharePointSite,
) -> None:
    sp_site, patched_requests_get, patched_requests_response = (
        patched_sp_site.instance,
        patched_sp_site.patched_requests_get,
        patched_sp_site.patched_requests_response,
    )

    patched_requests_response.reset_mock()
    patched_requests_response.json.return_value = {"id": SharePointTestSettings.DRIVE_ID}
    patched_requests_get.reset_mock()
    patched_requests_get.return_value = patched_requests_response

    drive = sp_site.default_document_library()

    assert (
        patched_requests_get.call_args[0][0]
        == f"{sp_site.graph_client.api_url}/sites/{SharePointTestSettings.SITE_ID}/drive"
    )
    assert drive.id == SharePointTestSettings.DRIVE_ID


def test_drive_fetch_item_content_raises_excception_for_non_existant_file(
    patched_sp_site: PatchedSharePointSite,
):
    sp_site, patched_requests_get, patched_requests_response = (
        patched_sp_site.instance,
        patched_sp_site.patched_requests_get,
        patched_sp_site.patched_requests_response,
    )

    patched_requests_response.json.return_value = {"id": SharePointTestSettings.DRIVE_ID}
    patched_requests_get.return_value = patched_requests_response
    drive = sp_site.default_document_library()

    patched_requests_get.side_effect = RuntimeError("requests error")
    with pytest.raises(RuntimeError) as excinfo:
        drive.fetch_item_content("NotAFolder/NotAFile.txt")

    assert "requests error" in str(excinfo.value)


def test_drive_fetch_item_content_retrieves_content_of_existing_file_from_download_url(
    patched_sp_site: PatchedSharePointSite,
):
    sp_site, patched_requests_get, patched_requests_response = (
        patched_sp_site.instance,
        patched_sp_site.patched_requests_get,
        patched_sp_site.patched_requests_response,
    )

    patched_requests_response.json.return_value = {"id": SharePointTestSettings.DRIVE_ID}
    patched_requests_get.return_value = patched_requests_response
    drive = sp_site.default_document_library()

    test_download_url = "https://site.download.url"
    patched_requests_response.json.return_value = {
        "@microsoft.graph.downloadUrl": test_download_url
    }
    test_file_content = b"my file contents"
    patched_requests_response.content.return_value = test_file_content

    test_file_path = "Folder/file.txt"
    drive.fetch_item_content(test_file_path)

    assert patched_requests_get.call_count == 2
    assert (
        patched_requests_get.call_args[0][0]
        == f"{sp_site.graph_client.api_url}/sites/{SharePointTestSettings.DRIVE_ID}/root:/{test_file_path}"
    )
    assert patched_requests_get.call_args[1][0] == test_download_url
