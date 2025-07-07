import io
from dataclasses import dataclass
from typing import List

import requests

from .graphapi import GraphClientV1, GraphItem


@dataclass
class DriveItem(GraphItem):
    drive_id: str
    name: str


class DriveFileItem(DriveItem):
    """Represents a single M365 file item with downloadable content"""

    pass


class DriveFolderItem(DriveItem):
    """Represents a single M365 folder"""

    def get_file(self, file_path: str) -> DriveFileItem:
        """Return a single file item representing the file at the path relatieve to the drive root

        :param file_path: _description_
        :return: A DriveFileItem or raises
            - a requests.exception.RequestsException if the file could not be retrieved
            - a ValueError if the file_path exists but is a folder
        """
        response_json = self.graph_client.get(f"{Drive.ENDPOINT}/{self.drive_id}/root:/{file_path}")
        if "folder" not in response_json:
            return DriveFileItem(
                graph_client=self.graph_client,
                id=response_json["id"],
                drive_id=self.drive_id,
                name=file_path,
            )
        else:
            raise ValueError(f"Path '{file_path}' exists but is a folder.")

    # def glob(self, pattern: str) -> List[DriveFileItem]:
    #     """Return a list of file items matching the given glob pattern.

    #     This method does not support recursion or wildcards as folder names.

    #     :param pattern: A unix-shell pattern matching a set of files
    #     :return: A, possibly empty, list of DriveFileItem instances
    #     """
    #     # Steps:
    #     #   - list the contents
    #     #   - return the items whose name matches the pattern
    #     response = self.graph_client.get(
    #         f"{Drive.ENDPOINT}/{self.drive_id}/items/{self.id}/children"
    #     )


#    relative_path: str

# @property
# def content() -> io.BytesIO:


class Drive(GraphItem):
    ENDPOINT: str = "drives"

    def root(self) -> DriveFolderItem:
        response = self.graph_client.get(f"{self.ENDPOINT}/{self.id}/root")
        return DriveFolderItem(
            drive_id=self.id,
            id=response["id"],
            graph_client=self.graph_client,
            name=response["name"],
        )

    # def item_at(self, relative_path: str) -> DriveItem:
    #     """Return a DriveItem representing the item at the given path

    #     :param relative_path: Path from drive root to file
    #     :type relative_path: str
    #     :return: _description_
    #     :rtype: DriveItem
    #     """
    #     pass

    # def fetch_item_content(self, relative_path: str) -> io.BytesIO:
    #     """Fetch the content of a file item on a given relative path to the sharepoint site library

    #     :param relative_path: Path relative to the library root. The path will be url-encoded before being passsed to the graph API.
    #     """
    #     response = self.graph_client.get(f"{self.ENDPOINT}/{self.id}/root:/{relative_path}")
    #     download_url = response[GraphClientV1.Key.DOWNLOADURL]
    #     response = requests.get(download_url)
    #     response.raise_for_status()
    #     return io.BytesIO(response.content)
