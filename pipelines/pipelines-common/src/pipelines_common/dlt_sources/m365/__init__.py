"""Reads files from a SharePoint documents library"""

from typing import Any, ClassVar, Dict, Iterator, List
import urllib.parse as urlparser

from authlib.integrations.httpx_client import OAuth2Client
import dlt
from dlt.common.configuration.specs import configspec
from dlt.common.storages.fsspec_filesystem import FileItemDict, glob_files, MTIME_DISPATCH
from dlt.common.typing import TSecretStrValue
from dlt.extract import decorators
import httpx
from msgraphfs import MSGDriveFS

from .settings import DEFAULT_CHUNK_SIZE

# Add our MSGDriveFS protocol to the known modificaton time mappings
MSGDRIVEFS_PROTOCOL = MSGDriveFS.protocol[0]
MTIME_DISPATCH[MSGDRIVEFS_PROTOCOL] = MTIME_DISPATCH["file"]


@configspec
class M365CredentialsResource:
    """Support configuration of authentication with msal through dlt config mechanism"""

    # Constants
    base_url: ClassVar[str] = "https://graph.microsoft.com"
    api_url: ClassVar[str] = f"{base_url}/v1.0"
    default_scope: ClassVar[str] = f"{base_url}/.default"
    login_url: ClassVar[str] = "https://login.microsoftonline.com"

    # Instance variables
    tenant_id: str = None
    client_id: str = None
    client_secret: TSecretStrValue = None

    @property
    def authority_url(self) -> str:
        return f"{self.login_url}/{self.tenant_id}"

    @property
    def oauth2_token_endpoint(self) -> str:
        return f"{self.authority_url}/oauth2/v2.0/token"

    def fetch_token(self) -> Dict[str, Any]:
        """Acquire tokens via OAuth"""
        oauth_client = OAuth2Client(
            self.client_id,
            self.client_secret,
            token_endpoint=self.oauth2_token_endpoint,
            scope=self.default_scope,
        )
        response = oauth_client.fetch_token()
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

    def oauth2_client_params(self, token: Dict[str, Any]):
        """Return a set of parameters suitable for initializing an OAuth client"""
        return {
            "scope": " ".join(self.default_scope),
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "token": token,
            "token_endpoint": self.oauth2_token_endpoint,
        }


def _get_site_drive_id(site_url: str, access_token: str) -> str:
    """Get the SharePoint site library drive id from the url

    :param site_url: Full url of the main page of the SharePoint site
    :param access_token: A Bearer access token to access the resource
    :return: The id property of the site's drive
    :raises: A requests.exception if an error code is encountered
    """

    # Two steps needed:
    #  - get the site ID from the path
    #  - get the drive ID from the /drive resource of the site
    def _get_id(endpoint_url: str) -> str:
        params = {"$select": "id"}
        response = httpx.get(
            endpoint_url,
            headers={"Authorization": f"Bearer {access_token}"},
            params=params,
        )
        response.raise_for_status()
        return response.json()["id"]

    urlparts = urlparser.urlparse(site_url)
    site_id = _get_id(f"{M365CredentialsResource.api_url}/sites/{urlparts.netloc}:{urlparts.path}")
    return _get_id(f"{M365CredentialsResource.api_url}/sites/{site_id}/drive")


# This is designed to look similar to the dlt.filesystem resource where the resource returns DriveItem
# objects that include the content as raw bytes. The bytes need to be parsed by an appropriate
# transformer
@decorators.resource(standalone=True)
def sharepoint(
    site_url: str = dlt.config.value,
    credentials: M365CredentialsResource = dlt.secrets.value,
    file_glob: str = dlt.config.value,
    files_per_page: int = DEFAULT_CHUNK_SIZE,
    extract_content: bool = False,
) -> Iterator[List[FileItemDict]]:
    """A dlt resource to pull files stored in a SharePoint document library.

    :param site_url: The absolute url to the main page of the SharePoint site
    :param file_glob: A glob pattern, relative to the site's document library root.
                       For example, if a file called 'file_to_ingest.csv' exists in the "Documents"
                       library in folder 'incoming' then the file_path would be '/incoming/file_to_ingest.csv'
    :return: List[DltResource]: A list of DriveItems representing the file
    """
    oauth_token = credentials.fetch_token()
    access_token = oauth_token["access_token"]
    sp_library = MSGDriveFS(
        drive_id=_get_site_drive_id(site_url, access_token),
        oauth2_client_params=credentials.oauth2_client_params(oauth_token),
    )

    files_chunk: List[FileItemDict] = []
    for file_model in glob_files(
        sp_library, bucket_url=f"{MSGDRIVEFS_PROTOCOL}:/", file_glob=file_glob
    ):
        file_dict = FileItemDict(file_model, credentials=sp_library)
        if extract_content:
            file_dict["file_content"] = file_dict.read_bytes()
        files_chunk.append(file_dict)

        # wait for the chunk to be full
        if len(files_chunk) >= files_per_page:
            yield files_chunk
            files_chunk = []

    if files_chunk:
        yield files_chunk
