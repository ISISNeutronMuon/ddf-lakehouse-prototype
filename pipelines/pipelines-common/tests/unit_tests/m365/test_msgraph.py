from pipelines_common.m365.msgraph import MSGraphV1, MsalCredentials

import pytest
from pytest_mock import MockerFixture
from requests_mock.mocker import Mocker as RequestsMocker

from unit_tests.m365.conftest import (
    MSGraphTestSettings,
    patch_msal_with_access_token,
    patch_msal_to_return,
)


def test_initialization_stores_credentials() -> None:
    credentials = MsalCredentials("ten1", "cli1", "s3c1")
    graph_client = MSGraphV1(credentials)

    assert graph_client.credentials == credentials


def test_acquire_token_raises_runtimeerror_when_error_in_response(
    graph_client: MSGraphV1,
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
    graph_client: MSGraphV1, mocker: MockerFixture
) -> None:
    patched_client_app = patch_msal_with_access_token(mocker)

    access_token = graph_client.acquire_token()

    patched_client_app.assert_called_once()
    assert access_token == MSGraphTestSettings.ACCESS_TOKEN


def test_get_prepends_api_url_to_endpoint(
    graph_client_with_access_token: MSGraphV1, requests_mock: RequestsMocker
):
    expected_url = f"{graph_client_with_access_token.api_url}/sites/MySite"
    requests_mock.get(expected_url, json={"access_code": MSGraphTestSettings.ACCESS_TOKEN})

    graph_client_with_access_token.get("sites/MySite")

    assert requests_mock.call_count == 1
    assert requests_mock.request_history[0].url == expected_url
    assert "Authorization" in requests_mock.request_history[0].headers
