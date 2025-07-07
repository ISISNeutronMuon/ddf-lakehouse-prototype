import re
from pipelines_common.m365.graphapi import GraphClientV1, MsalCredentials

import pytest
from pytest_mock import MockerFixture
import requests
from requests_mock.mocker import Mocker as RequestsMocker

from unit_tests.m365.conftest import (
    MSGraphTestSettings,
    patch_msal_with_access_token,
    patch_msal_to_return,
)
from unit_tests.m365.conftest import SharePointTestSettings


def test_initialization_stores_credentials() -> None:
    credentials = MsalCredentials("ten1", "cli1", "s3c1")
    graph_client = GraphClientV1(credentials)

    assert graph_client.credentials == credentials


def test_acquire_token_raises_runtimeerror_when_error_in_response(
    graph_client: GraphClientV1,
    mocker: MockerFixture,
) -> None:
    patch_msal_to_return(
        {
            "error": "error_summary - error_code",
            "error_description": "human readable description",
        },
        mocker,
    )

    with pytest.raises(RuntimeError) as excinfo:
        graph_client.acquire_token()

    assert "error_code" in str(excinfo.value)


def test_acquire_token_returns_access_token_from_response_if_exists(
    graph_client: GraphClientV1, mocker: MockerFixture
) -> None:
    patched_client_app = patch_msal_with_access_token(mocker)

    access_token = graph_client.acquire_token()

    patched_client_app.assert_called_once()
    assert access_token == MSGraphTestSettings.ACCESS_TOKEN


def test_get_includes_auth_token(graph_client: GraphClientV1, requests_mock: RequestsMocker):
    expected_url = f"{graph_client.api_url}/sites/MySite"
    expected_response = {"key": "value"}
    requests_mock.get(expected_url, json=expected_response)

    response = graph_client.get("sites/MySite")

    assert requests_mock.call_count == 1
    assert requests_mock.request_history[0].url == expected_url
    assert "Authorization" in requests_mock.request_history[0].headers
    assert response == expected_response


def test_site_raises_when_request_raises_error(
    graph_client: GraphClientV1, requests_mock: RequestsMocker
) -> None:
    requests_mock.get(re.compile(".*"), exc=requests.exceptions.InvalidURL)

    with pytest.raises(requests.exceptions.InvalidURL):
        graph_client.site(weburl="https://name.host.com/sites/NotASite")


def test_site_returns_site_instance_for_valid_url(
    graph_client: GraphClientV1, requests_mock: RequestsMocker
) -> None:
    requests_mock.get(
        SharePointTestSettings.site_api_url(graph_client),
        json={"id": SharePointTestSettings.SITE_ID},
    )

    site = graph_client.site(
        weburl=f"https://{SharePointTestSettings.HOSTNAME}/{SharePointTestSettings.SITE_PATH}"
    )

    assert site.id == SharePointTestSettings.SITE_ID
