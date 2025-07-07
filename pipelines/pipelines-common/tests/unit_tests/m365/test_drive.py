from pipelines_common.m365.drive import Drive as M365Drive
from pipelines_common.m365.graphapi import GraphClientV1

import pytest
import requests
from requests_mock.mocker import Mocker as RequestsMocker

from unit_tests.m365.conftest import SharePointTestSettings


@pytest.fixture
def m365_drive(graph_client_with_access_token: GraphClientV1) -> M365Drive:
    return M365Drive(
        graph_client=graph_client_with_access_token, id=SharePointTestSettings.DRIVE_ID
    )


def test_fetch_item_content_raises_exception_for_non_existant_file(
    m365_drive: M365Drive, requests_mock: RequestsMocker
):
    non_existing_file_path = "NotAFolder/NotAFile.txt"
    requests_mock.get(
        SharePointTestSettings.site_library_item_api_url(
            m365_drive.graph_client, non_existing_file_path
        ),
        exc=requests.exceptions.HTTPError("404"),
    )

    with pytest.raises(requests.exceptions.HTTPError) as excinfo:
        m365_drive.fetch_item_content(non_existing_file_path)

    assert "404" in str(excinfo.value)


def test_fetch_item_content_retrieves_content_of_existing_file_from_download_url(
    m365_drive: M365Drive, requests_mock: RequestsMocker
):
    existing_file_path, existing_file_content = "SubFolder/AFile.txt", b"some test file content"
    test_downloadurl = "https://test.download.com/34534efwef54"
    requests_mock.get(
        SharePointTestSettings.site_library_item_api_url(
            m365_drive.graph_client, existing_file_path
        ),
        json={GraphClientV1.Key.DOWNLOADURL: test_downloadurl},
    )
    requests_mock.get(test_downloadurl, content=existing_file_content)

    fetched_file_content = m365_drive.fetch_item_content(existing_file_path)

    assert fetched_file_content != fetched_file_content.getvalue()
