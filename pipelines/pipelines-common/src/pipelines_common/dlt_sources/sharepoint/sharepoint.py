import io
import requests

from .msgraph import MSGraphV1


class MSGraphAccessor:
    def __init__(self, graph_client: MSGraphV1) -> None:
        self.graph_client = graph_client


class M365Drive(MSGraphAccessor):
    ENDPOINT: str = "drives"

    def __init__(self, graph_client: MSGraphV1, drive_id: str) -> None:
        super().__init__(graph_client)
        self._id = drive_id

    @property
    def id(self):
        return self._id

    def fetch_item_content(self, relative_path: str) -> io.BytesIO:
        """Fetch the content of a file item on a given relative path to the drive

        :param relative_path: Path relative to the drive root. The path will be url-encoded before being passsed to the graph API.
        """
        response = self.graph_client.get(f"{self.ENDPOINT}/{self.id}/root:/{relative_path}")
        download_url = response["@microsoft.graph.downloadUrl"]
        response = requests.get(download_url)
        response.raise_for_status()
        return io.BytesIO(response.content)


class SharePointSite(MSGraphAccessor):
    ENDPOINT: str = "sites"

    def __init__(self, graph_client: MSGraphV1, hostname: str, relative_path: str) -> None:
        super().__init__(graph_client)
        response = graph_client.get(f"{self.ENDPOINT}/{hostname}:/{relative_path}")
        self._id = response["id"]

    @property
    def id(self):
        return self._id

    def default_document_library(self) -> M365Drive:
        """Retrieve the default SharePoint document drive"""
        response = self.graph_client.get(f"{self.ENDPOINT}/{self.id}/drive")
        return M365Drive(self.graph_client, response["id"])
