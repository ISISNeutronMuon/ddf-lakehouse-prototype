import pytest
from pytest_mock import MockerFixture

from pipelines_common.sharepoint.msgraph import (
    MSGraphV1,
)


class MSGraphTestSettings:
    TENANT_ID: str = "tenant1"
    CLIENT_ID: str = "client1"
    CLIENT_SECRET: str = "clients3cr3t"
    ACCESS_TOKEN: str = "0987654321abcdefg"


class SharePointTestSettings:
    HOSTNAME: str = "site.domain.com"
    SITE_PATH: str = "sites/MySite"
    SITE_ID: str = (
        f"{HOSTNAME},0000000-1111-2222-3333-444444444444,55555555-6666-7777-8888-99999999999"
    )
    DRIVE_ID: str = "K9z4mkx?yn:zIBXWxeAT%PiZKNg7-ZcycW81a4B%.I9DT%l}@8o5sGm',kdpr4L__nI"


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
        "pipelines_common.sharepoint.msgraph.ConfidentialClientApplication"
    )
    patched_client_app.return_value.acquire_token_for_client.return_value = msal_response

    return patched_client_app
