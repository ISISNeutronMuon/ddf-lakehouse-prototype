import pytest
from pytest_mock import MockerFixture

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


@pytest.fixture
def graph_client_with_access_token(graph_client: MSGraphV1, mocker: MockerFixture) -> MSGraphV1:
    patch_msal_with_access_token(mocker)
    return graph_client


def patch_msal_with_access_token(mocker: MockerFixture):
    return patch_msal_to_return({"access_token": MSGraphTestSettings.ACCESS_TOKEN}, mocker)


def patch_msal_to_return(msal_response: dict, mocker: MockerFixture):
    patched_client_app = mocker.patch(
        "pipelines_common.dlt_sources.sharepoint.msgraph.ConfidentialClientApplication"
    )
    patched_client_app.return_value.acquire_token_for_client.return_value = msal_response

    return patched_client_app
