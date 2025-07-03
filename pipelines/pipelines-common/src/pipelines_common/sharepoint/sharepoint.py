import io
import requests

from .msgraph import MSGraphV1


class SharePointSite:
    ENDPOINT: str = "sites"

    def __init__(self, graph_client: MSGraphV1, hostname: str, relative_path: str) -> None:
        self.graph_client = graph_client
        response = graph_client.get(f"{self.ENDPOINT}/{hostname}:/{relative_path}")
        self._id = response[MSGraphV1.Key.ID]

    @property
    def id(self):
        return self._id

    def fetch_library_item_content(self, relative_path: str) -> io.BytesIO:
        """Fetch the content of a file item on a given relative path to the sharepoint site library

        :param relative_path: Path relative to the library root. The path will be url-encoded before being passsed to the graph API.
        """
        response = self.graph_client.get(f"{self.ENDPOINT}/{self.id}/drive/root:/{relative_path}")
        download_url = response[MSGraphV1.Key.DOWNLOADURL]
        response = requests.get(download_url)
        response.raise_for_status()
        return io.BytesIO(response.content)
