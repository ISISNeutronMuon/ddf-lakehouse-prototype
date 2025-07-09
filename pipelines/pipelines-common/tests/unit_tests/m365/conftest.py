from pathlib import Path

import pytest
from pytest_mock import MockerFixture
from requests_mock.mocker import Mocker as RequestsMocker

from pipelines_common.m365.graphapi import (
    MsalCredentials,
    GraphClientV1,
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
    LIBRARY_ID: str = "b!umnFmiec4Blh4I3Cv5be8uPs4IEW9cGoyL2iMLaafz2yjlrRtGbxqbR31mJiv5hCZfL"

    @classmethod
    def site_api_url(cls) -> str:
        return f"{GraphClientV1.api_url}/sites/{cls.HOSTNAME}:/{cls.SITE_PATH}"

    @classmethod
    def site_library_api_url(cls) -> str:
        return f"{GraphClientV1.api_url}/sites/{SharePointTestSettings.SITE_ID}/drive"

    @classmethod
    def mock_requests_for_file(
        cls, requests_mock: RequestsMocker, file_path: str, file_content: bytes
    ):
        requests_mock.get(cls.site_api_url(), json={"id": cls.SITE_ID})
        requests_mock.get(cls.site_library_api_url(), json={"id": cls.LIBRARY_ID})
        requests_mock.get(
            f"{GraphClientV1.api_url}/drives/{cls.LIBRARY_ID}/root",
            json={"id": cls.LIBRARY_ID, "name": "root"},
        )
        download_url = "https://file.download.url/458972349iuf"
        requests_mock.get(
            f"{GraphClientV1.api_url}/drives/{cls.LIBRARY_ID}/root:/{file_path}",
            json={
                "id": cls.LIBRARY_ID,
                "name": Path(file_path).name,
                "@microsoft.graph.downloadUrl": download_url,
            },
        )
        requests_mock.get(download_url, content=file_content)


class DriveTestSettings:
    ID: str = "b!ejiYvIQW9PJmAu7cuils0N8UUvV7zURwlBMY4DJi1NRD58OMbRFXjb16RGKIn5ujQ"

    @classmethod
    def root_api_url(cls) -> str:
        return f"{GraphClientV1.api_url}/drives/{cls.ID}/root"

    @classmethod
    def item_api_url(cls, path: str) -> str:
        return f"{GraphClientV1.api_url}/drives/{cls.ID}/root:/{path}"


@pytest.fixture
def graph_client(mocker: MockerFixture) -> GraphClientV1:
    patch_msal_with_access_token(mocker)
    return GraphClientV1(
        MsalCredentials(
            MSGraphTestSettings.TENANT_ID,
            MSGraphTestSettings.CLIENT_ID,
            MSGraphTestSettings.CLIENT_SECRET,
        )
    )


def patch_msal_with_access_token(mocker: MockerFixture):
    return patch_msal_to_return({"access_token": MSGraphTestSettings.ACCESS_TOKEN}, mocker)


def patch_msal_to_return(msal_response: dict, mocker: MockerFixture):
    patched_client_app = mocker.patch(
        "pipelines_common.m365.graphapi.ConfidentialClientApplication"
    )
    patched_client_app.return_value.acquire_token_for_client.return_value = msal_response

    return patched_client_app
