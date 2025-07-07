import dataclasses
from typing import Any, Dict, TYPE_CHECKING
import urllib.parse as urlparser

from msal import ConfidentialClientApplication
import requests

if TYPE_CHECKING:
    from .sharepoint import Site


@dataclasses.dataclass
class MsalCredentials:
    tenant_id: str
    client_id: str
    client_secret: str


@dataclasses.dataclass
class GraphClientV1:
    class Key:
        ID: str = "id"
        DOWNLOADURL = "@microsoft.graph.downloadUrl"

    credentials: MsalCredentials

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
            authority=self.authority_url(self.credentials.tenant_id),
            client_id=f"{self.credentials.client_id}",
            client_credential=f"{self.credentials.client_secret}",
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

    def site(self, weburl: str) -> "Site":
        """Get the SharePoint site at the given web url

        :param weburl: Url shown in the browser of the frontpage of the site
        :return: A Site object representing the site
        """
        from .sharepoint import Site

        urlparts = urlparser.urlparse(weburl)
        response = self.get(f"{Site.ENDPOINT}/{urlparts.netloc}:{urlparts.path}")
        return Site(graph_client=self, id=response["id"])

    # ----- private -----
    def _add_bearer_token_header(self, headers: Dict[str, str], token: str) -> Dict[str, str]:
        headers.update({"Authorization": f"Bearer {token}"})
        return headers


@dataclasses.dataclass
class GraphItem:
    id: str
    graph_client: GraphClientV1
