from pipelines_common.dlt_sources.sharepoint.msgraph import (
    MSGraphV1,
)

import pytest
from pytest_mock import MockerFixture

from unit_tests.dlt_sources.sharepoint.conftest import (
    MSGraphTestSettings,
    patch_msal_with_access_token,
    patch_msal_to_return,
)


def test_initialization_stores_variables_in_expected_fields() -> None:
    tenant_id, client_id, client_secret = "ten1", "cli1", "s3c1"
    graph_client = MSGraphV1(tenant_id, client_id, client_secret)

    assert graph_client.tenant_id == tenant_id
    assert graph_client.client_id == client_id
    assert graph_client.client_secret == client_secret


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
    graph_client_with_access_token: MSGraphV1, mocker: MockerFixture
):
    patched_requests = mocker.patch("pipelines_common.dlt_sources.sharepoint.msgraph.requests")

    graph_client_with_access_token.get("sites/MySite")

    assert patched_requests.get.call_count == 1
    assert (
        patched_requests.get.call_args[0][0]
        == f"{graph_client_with_access_token.api_url}/sites/MySite"
    )
    assert "headers" in patched_requests.get.call_args[1]
    assert "Authorization" in patched_requests.get.call_args[1]["headers"]
