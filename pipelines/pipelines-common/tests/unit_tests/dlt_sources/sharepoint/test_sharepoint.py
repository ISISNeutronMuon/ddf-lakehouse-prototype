from pipelines_common.dlt_sources.sharepoint.msgraph import (
    MSGraphV1,
)
from pipelines_common.dlt_sources.sharepoint.sharepoint import SharePointSite

import pytest
from pytest_mock import MockerFixture


def test_sharepointsite_init_raises_error_request_raises_error(
    graph_client_with_access_token: MSGraphV1, mocker: MockerFixture
) -> None:
    patched_requests_get = mocker.patch(
        "pipelines_common.dlt_sources.sharepoint.sharepoint.requests.get"
    )
    patched_requests_get.side_effect = RuntimeError("requests error")

    with pytest.raises(Exception) as excinfo:
        SharePointSite(graph_client_with_access_token, "site.domain.com", "sites/NotASite")

    assert "requests error" in str(excinfo.value)


def test_sharepointsite_init_stores_id_of_site(
    graph_client_with_access_token: MSGraphV1, mocker: MockerFixture
) -> None:
    hostname, relative_path = "site.domain.com", "sites/MySite"
    mock_id = f"{hostname},0000000-1111-2222-3333-444444444444,55555555-6666-7777-8888-99999999999"

    patched_requests_response = mocker.patch(
        "pipelines_common.dlt_sources.sharepoint.sharepoint.requests.Response"
    )
    patched_requests_response.json.return_value = {"id": mock_id}
    patched_requests_get = mocker.patch(
        "pipelines_common.dlt_sources.sharepoint.sharepoint.requests.get"
    )
    patched_requests_get.return_value = patched_requests_response

    sp_site = SharePointSite(graph_client_with_access_token, hostname, relative_path)

    patched_requests_get.assert_called_once()
    assert (
        patched_requests_get.call_args[0][0]
        == f"{graph_client_with_access_token.api_url}/sites/{hostname}:/sites/MySite"
    )
    assert sp_site.id == mock_id


def test_sharepoint_default_document_library_produces_drive_with_expected_id(
    graph_client_with_access_token: MSGraphV1, mocker: MockerFixture
) -> None:
    hostname, relative_path = "site.domain.com", "sites/MySite"
    mock_site_id = (
        f"{hostname},0000000-1111-2222-3333-444444444444,55555555-6666-7777-8888-99999999999"
    )
    mock_drive_id = "K9z4mkx?yn:zIBXWxeAT%PiZKNg7-ZcycW81a4B%.I9DT%l}@8o5sGm',kdpr4L__nI"
    patched_requests_response = mocker.patch(
        "pipelines_common.dlt_sources.sharepoint.sharepoint.requests.Response"
    )
    patched_requests_get = mocker.patch(
        "pipelines_common.dlt_sources.sharepoint.sharepoint.requests.get"
    )

    patched_requests_response.json.return_value = {"id": mock_site_id}
    patched_requests_get.return_value = patched_requests_response
    sp_site = SharePointSite(graph_client_with_access_token, hostname, relative_path)
    assert sp_site.id == mock_site_id

    patched_requests_response.reset_mock()
    patched_requests_response.json.return_value = {"id": mock_drive_id}
    patched_requests_get.reset_mock()
    patched_requests_get.return_value = patched_requests_response

    drive = sp_site.default_document_library()

    assert (
        patched_requests_get.call_args[0][0]
        == f"{graph_client_with_access_token.api_url}/sites/{mock_site_id}/drive"
    )
    assert drive.id == mock_drive_id
