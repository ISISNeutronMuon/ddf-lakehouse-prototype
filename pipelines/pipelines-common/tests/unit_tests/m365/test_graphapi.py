import re
import urllib.parse
from pipelines_common.m365.graphapi import GraphClientV1, GraphCredentials

import pytest
from pytest_mock import MockerFixture
import httpx
from pytest_httpx import HTTPXMock

from unit_tests.m365.conftest import (
    MSGraphTestSettings,
    patch_outh_client_with_access_token,
    patch_oauth_client_to_return,
)
from unit_tests.m365.conftest import SharePointTestSettings


def test_initialization_stores_credentials() -> None:
    credentials = GraphCredentials("ten1", "cli1", "s3c1")
    graph_client = GraphClientV1(credentials)

    assert graph_client.credentials == credentials


def test_acquire_token_raises_runtimeerror_when_error_in_response(
    graph_client: GraphClientV1,
    mocker: MockerFixture,
) -> None:
    patch_oauth_client_to_return(
        {
            "error": "error_summary - error_code",
            "error_description": "human readable description",
        },
        mocker,
    )

    with pytest.raises(RuntimeError) as excinfo:
        graph_client.fetch_token()

    assert "error_code" in str(excinfo.value)


def test_acquire_tokens_returns_tokens_from_response_if_exists(
    graph_client: GraphClientV1, mocker: MockerFixture
) -> None:
    patched_client_app = patch_outh_client_with_access_token(mocker)

    tokens = graph_client.fetch_token()

    patched_client_app.assert_called_once()
    assert tokens["access_token"] == MSGraphTestSettings.ACCESS_TOKEN


def test_get_includes_auth_token(graph_client: GraphClientV1, httpx_mock: HTTPXMock):
    expected_url = f"{graph_client.api_url}/sites/MySite"
    expected_response = {"key": "value"}
    httpx_mock.add_response(method="GET", url=expected_url, json=expected_response)

    response = graph_client.get("sites/MySite")

    assert httpx_mock.get_requests()[-1].url == expected_url
    assert "Authorization" in httpx_mock.get_requests()[-1].headers
    assert response == expected_response


def test_get_with_select_adds_select_odata_query_parameter(
    graph_client: GraphClientV1, httpx_mock: HTTPXMock
):
    expected_url = f"{graph_client.api_url}/sites/MySite"
    expected_query_params = {"$select": "key1,key2"}
    expected_response = {"key1": "value1", "key2": "value2"}
    httpx_mock.add_response(
        method="GET",
        url=httpx.URL(expected_url, params=expected_query_params),
        json=expected_response,
    )

    response = graph_client.get("sites/MySite", select=("key1", "key2"))

    assert (
        httpx_mock.get_requests()[-1].url
        == f"{expected_url}?{urllib.parse.urlencode(expected_query_params)}"
    )
    assert response == expected_response


def test_site_raises_when_request_raises_error(
    graph_client: GraphClientV1, httpx_mock: HTTPXMock
) -> None:
    httpx_mock.add_response(method="GET", url=re.compile(".*"), status_code=404)

    with pytest.raises(httpx.HTTPError):
        graph_client.site(weburl="https://name.host.com/sites/NotASite")


def test_site_returns_site_instance_for_valid_url(
    graph_client: GraphClientV1, httpx_mock: HTTPXMock
) -> None:
    httpx_mock.add_response(
        method="GET",
        url=SharePointTestSettings.site_api_url(),
        json={"id": SharePointTestSettings.SITE_ID},
    )

    site = graph_client.site(
        weburl=f"https://{SharePointTestSettings.HOSTNAME}/{SharePointTestSettings.SITE_PATH}"
    )

    assert site.id == SharePointTestSettings.SITE_ID
