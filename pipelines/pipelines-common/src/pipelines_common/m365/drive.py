import io
from dataclasses import dataclass

import requests

from .graphapi import GraphClientV1, GraphItem


@dataclass
class DriveItem:
    """Represents a single M365 drive item"""

    id: str
    file_name: str
    relative_path: str
    file_content: bytes


class Drive(GraphItem):
    ENDPOINT: str = "drives"

    def fetch_item_content(self, relative_path: str) -> io.BytesIO:
        """Fetch the content of a file item on a given relative path to the sharepoint site library

        :param relative_path: Path relative to the library root. The path will be url-encoded before being passsed to the graph API.
        """
        response = self.graph_client.get(f"{self.ENDPOINT}/{self.id}/root:/{relative_path}")
        download_url = response[GraphClientV1.Key.DOWNLOADURL]
        response = requests.get(download_url)
        response.raise_for_status()
        return io.BytesIO(response.content)
