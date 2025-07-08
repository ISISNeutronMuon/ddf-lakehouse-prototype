"""Reads files from a SharePoint documents library"""

from typing import Iterator, List

import dlt
from dlt.extract import decorators

from dlt.common.configuration.specs import configspec
from dlt.common.typing import TSecretStrValue

from pipelines_common.m365.graphapi import GraphClientV1, MsalCredentials
from pipelines_common.m365.drive import DriveFileItem as M365DriveFileItem


@configspec
class MsalCredentialsResource:
    """Support configuration of authentication with msal through dlt config mechanism"""

    tenant_id: str = None
    client_id: str = None
    client_secret: TSecretStrValue = None

    def as_msal(self) -> MsalCredentials:
        return MsalCredentials(self.tenant_id, self.client_id, self.client_secret)


# This is designed to look similar to the dlt.filesystem resource where the resource returns DriveItem
# objects that include the content as raw bytes. The bytes need to be parsed by an appropriate
# transformer
@decorators.resource(standalone=True)
def sharepoint(
    site_url: str = dlt.config.value,
    credentials: MsalCredentialsResource = dlt.secrets.value,
    file_glob: str = dlt.config.value,
) -> Iterator[List[M365DriveFileItem]]:
    """A dlt resource to pull files stored in a SharePoint document library.

    :param site_url: The absolute url to the main page of the SharePoint site
    :param file_glob: A glob pattern, relative to the site's document library root.
                       For example, if a file called 'file_to_ingest.csv' exists in the "Documents"
                       library in folder 'incoming' then the file_path would be 'incoming/file_to_ingest.csv'
    :return: List[DltResource]: A list of DriveItems representing the file
    """
    if any(char in file_glob for char in ("*", "?", "[")):
        raise NotImplementedError("Glob syntax not implmented...")

    graph_client = GraphClientV1(credentials=credentials.as_msal())
    site = graph_client.site(site_url)

    yield [site.document_library().root().get_file(file_glob)]
