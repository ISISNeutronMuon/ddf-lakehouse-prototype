import dataclasses
from typing import Any, ClassVar, Dict, TYPE_CHECKING, Sequence
import urllib.parse as urlparser

import httpx
from authlib.integrations.httpx_client import OAuth2Client

if TYPE_CHECKING:
    from .sharepoint import Site


@dataclasses.dataclass
class GraphCredentials:
    tenant_id: str
    client_id: str
    client_secret: str


class GraphClientV1:
    # Constants
    base_url: ClassVar[str] = "https://graph.microsoft.com"
    api_url: ClassVar[str] = f"{base_url}/v1.0"
    default_scope: ClassVar[str] = f"{base_url}/.default"
    login_url: ClassVar[str] = "https://login.microsoftonline.com"

    @property
    def authority_url(self) -> str:
        return f"{self.login_url}/{self.credentials.tenant_id}"

    def __init__(self, credentials: GraphCredentials) -> None:
        self.credentials = credentials
        self._oauth_client_value = None

    def oauth2_token_endpoint(self) -> str:
        return f"{self.authority_url}/oauth2/v2.0/token"

    def fetch_token(self) -> Dict[str, Any]:
        """Acquire tokens via OAuth"""
        response = self._oauth_client.fetch_token()
        if response:
            if "access_token" in response:
                return response
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

    def oauth2_client_params(self):
        """Return a set of parameters suitable for initializing an OAuth client"""
        return {
            "scope": " ".join(self.default_scope),
            "client_id": self.credentials.client_id,
            "client_secret": self.credentials.client_secret,
            "token": self.fetch_token(),
            "token_endpoint": self.oauth2_token_endpoint(),
        }

    def get(self, endpoint: str, select: Sequence[str] | None = None) -> Dict[str, Any]:
        """Get the requested endpoint. Raises a requests exception if an error occurred.

        :param endpoint: Endpoint relative to the graph api URL
        :param select: An optional list of response properties.
                       The response will only include these properties instead of the full response.
        :return: The response JSON decoded as a dict
        :raises: A requests.exception if an error code is encountered
        """
        response = httpx.get(
            f"{self.api_url}/{endpoint}:",
            headers=self._add_bearer_token_header({}, self.fetch_token()["access_token"]),
            params={"select": ",".join(select)} if select else None,
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
        response = self.get(f"{Site.endpoint}/{urlparts.netloc}:{urlparts.path}")
        return Site(graph_client=self, id=response["id"])

    # ----- private -----
    @property
    def _oauth_client(self):
        if self._oauth_client_value is None:
            self._oauth_client_value = OAuth2Client(
                self.credentials.client_id,
                self.credentials.client_secret,
                token_endpoint=self.oauth2_token_endpoint(),
                scope=GraphClientV1.default_scope,
            )
        return self._oauth_client_value

    def _add_bearer_token_header(self, headers: Dict[str, str], token: str) -> Dict[str, str]:
        headers.update({"Authorization": f"Bearer {token}"})
        return headers


@dataclasses.dataclass
class GraphItem:
    graph_client: GraphClientV1
    id: str
