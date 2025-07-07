from pipelines_common.m365.graphapi import (
    GraphClientV1,
)
from pipelines_common.m365.sharepoint import Site

import pytest
import requests
from requests_mock.mocker import Mocker as RequestsMocker

from unit_tests.m365.conftest import SharePointTestSettings


@pytest.fixture
def sharepoint_site(graph_client_with_access_token: GraphClientV1) -> Site:
    return Site(graph_client=graph_client_with_access_token, id=SharePointTestSettings.SITE_ID)


def test_sharepointsite_init_stores_id_of_site(sharepoint_site: Site) -> None:
    assert sharepoint_site.id == SharePointTestSettings.SITE_ID


def test_fetch_item_content_raises_exception_for_non_existant_file(
    sharepoint_site: Site, requests_mock: RequestsMocker
):
    non_existing_file_path = "NotAFolder/NotAFile.txt"
    requests_mock.get(
        SharePointTestSettings.site_library_item_api_url(
            sharepoint_site.graph_client, non_existing_file_path
        ),
        exc=requests.exceptions.HTTPError("404"),
    )

    with pytest.raises(requests.exceptions.HTTPError) as excinfo:
        sharepoint_site.fetch_library_item_content(non_existing_file_path)

    assert "404" in str(excinfo.value)


def test_fetch_item_content_retrieves_content_of_existing_file_from_download_url(
    sharepoint_site: Site, requests_mock: RequestsMocker
):
    existing_file_path, existing_file_content = "SubFolder/AFile.txt", b"some test file content"
    test_downloadurl = "https://test.download.com/34534efwef54"
    requests_mock.get(
        SharePointTestSettings.site_library_item_api_url(
            sharepoint_site.graph_client, existing_file_path
        ),
        json={GraphClientV1.Key.DOWNLOADURL: test_downloadurl},
    )
    requests_mock.get(test_downloadurl, content=existing_file_content)

    fetched_file_content = sharepoint_site.fetch_library_item_content(existing_file_path)

    assert fetched_file_content != fetched_file_content.getvalue()
