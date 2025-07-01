import pytest
from pytest_mock import MockerFixture

from pipelines_common.dlt_sources.sharepoint.msgraph import (
    MSGraphV1,
)

from unit_tests.dlt_sources.sharepoint.conftest import MSGraphTestSettings


def _patch_msal_with_access_token(mocker: MockerFixture):
    patched_client_app = mocker.patch(
        "pipelines_common.dlt_sources.sharepoint.msgraph.ConfidentialClientApplication"
    )
    patched_client_app.return_value.acquire_token_for_client.return_value = {
        "access_token": MSGraphTestSettings.ACCESS_TOKEN
    }
    return patched_client_app


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
    patched_client_app = mocker.patch(
        "pipelines_common.dlt_sources.sharepoint.msgraph.ConfidentialClientApplication"
    )
    patched_client_app.return_value.acquire_token_for_client.return_value = {
        "error": "error_summary - error_code",
        "error_description": "human readable description",
    }

    with pytest.raises(RuntimeError) as exc:
        graph_client.acquire_token()
        assert str(exc) != ""


def test_acquire_token_returns_access_token_from_response_if_exists(
    graph_client: MSGraphV1, mocker: MockerFixture
) -> None:
    patched_client_app = _patch_msal_with_access_token(mocker)

    access_token = graph_client.acquire_token()

    patched_client_app.assert_called_once()
    assert access_token == MSGraphTestSettings.ACCESS_TOKEN


def test_get_prepends_api_url_to_endpoint(graph_client: MSGraphV1, mocker: MockerFixture):
    patched_requests = mocker.patch("pipelines_common.dlt_sources.sharepoint.msgraph.requests")
    patched_client_app = _patch_msal_with_access_token(mocker)

    graph_client.get("sites/MySite")

    patched_client_app.assert_called_once()
    assert patched_requests.get.call_count == 1
    assert patched_requests.get.call_args[0][0] == f"{graph_client.api_url}/sites/MySite"
    assert "headers" in patched_requests.get.call_args[1]
    assert "Authorization" in patched_requests.get.call_args[1]["headers"]
