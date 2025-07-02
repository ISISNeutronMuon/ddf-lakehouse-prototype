import dataclasses
from typing import Any, Dict

from msal import ConfidentialClientApplication
import requests


@dataclasses.dataclass
class MSGraphV1:
    tenant_id: str
    client_id: str
    client_secret: str

    @property
    def base_url(self) -> str:
        return "https://graph.microsoft.com"

    @property
    def api_url(self) -> str:
        return f"{self.base_url}/v1.0"

    @property
    def default_scope(self) -> str:
        return f"{self.base_url}/.default"

    def authority_url(self, tenant_id: str) -> str:
        return f"https://login.microsoftonline.com/{tenant_id}"

    def acquire_token(self) -> str:
        """Acquire token via MSAL library"""
        app = ConfidentialClientApplication(
            authority=self.authority_url(self.tenant_id),
            client_id=f"{self.client_id}",
            client_credential=f"{self.client_secret}",
        )
        response = app.acquire_token_for_client(scopes=[self.default_scope])
        if response:
            if "access_token" in response:
                return response["access_token"]
            elif "error" in response:
                raise RuntimeError(
                    f"Error acquiring graph client token: {response['error']} - {response['error_description']}"
                )
            else:
                raise RuntimeError(
                    "Error acquiring graph client token: No error description found in the response."
                )
        else:
            raise RuntimeError("Error acquiring graph client token. No response found.")

    def get(self, endpoint: str) -> Dict[str, Any]:
        """Get the requested endpoint. Raises a requests exception if an error occurred.

        :param endpoint: Endpoint relative to the graph api URL
        :return: The response JSON decoded as a dict
        :raises: A requests.exception if an error code is encountered
        """
        response = requests.get(
            f"{self.api_url}/{endpoint}",
            headers=self._add_bearer_token_header({}, self.acquire_token()),
        )
        response.raise_for_status()
        return response.json()

    # ----- private -----
    def _add_bearer_token_header(self, headers: Dict[str, str], token: str) -> Dict[str, str]:
        headers.update({"Authorization": f"Bearer {token}"})
        return headers
