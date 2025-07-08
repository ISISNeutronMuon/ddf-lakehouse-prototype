from pipelines_common.m365.graphapi import (
    GraphClientV1,
)
from pipelines_common.m365.sharepoint import Site

from requests_mock.mocker import Mocker as RequestsMocker
from unit_tests.m365.conftest import SharePointTestSettings


def test_site_init(
    graph_client: GraphClientV1,
) -> None:
    sharepoint_site = Site(graph_client=graph_client, id=SharePointTestSettings.SITE_ID)

    assert sharepoint_site.id == SharePointTestSettings.SITE_ID


def test_site_document_library_returns_drive_instance(
    graph_client: GraphClientV1, requests_mock: RequestsMocker
) -> None:
    sharepoint_site = Site(graph_client=graph_client, id=SharePointTestSettings.SITE_ID)
    requests_mock.get(
        SharePointTestSettings.site_library_api_url(),
        json={"id": SharePointTestSettings.LIBRARY_ID},
    )

    drive = sharepoint_site.document_library()

    assert drive.id == SharePointTestSettings.LIBRARY_ID
