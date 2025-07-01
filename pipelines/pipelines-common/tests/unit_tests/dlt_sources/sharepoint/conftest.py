import pytest

from pipelines_common.dlt_sources.sharepoint.msgraph import (
    MSGraphV1,
)


class MSGraphTestSettings:
    TENANT_ID = "tenant1"
    CLIENT_ID = "client1"
    CLIENT_SECRET = "clients3cr3t"
    ACCESS_TOKEN = "0987654321abcdefg"


@pytest.fixture
def graph_client() -> MSGraphV1:
    return MSGraphV1(
        MSGraphTestSettings.TENANT_ID,
        MSGraphTestSettings.CLIENT_ID,
        MSGraphTestSettings.CLIENT_SECRET,
    )
