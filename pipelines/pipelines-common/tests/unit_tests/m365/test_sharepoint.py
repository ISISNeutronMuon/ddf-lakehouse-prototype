from pipelines_common.m365.graphapi import (
    GraphClientV1,
)
from pipelines_common.m365.sharepoint import Site

from unit_tests.m365.conftest import SharePointTestSettings


def test_sharepointsite_init_stores_id_of_site(
    graph_client_with_access_token: GraphClientV1,
) -> None:
    sharepoint_site = Site(
        graph_client=graph_client_with_access_token, id=SharePointTestSettings.SITE_ID
    )
    assert sharepoint_site.id == SharePointTestSettings.SITE_ID
