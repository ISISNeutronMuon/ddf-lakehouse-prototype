from dataclasses import dataclass
from typing import List

import requests

from .graphapi import GraphItem


@dataclass
class DriveItem(GraphItem):
    drive_id: str
    name: str


@dataclass
class DriveFileItem(DriveItem):
    """Represents a single M365 file item with downloadable content"""

    download_url: str

    @property
    def content(self) -> bytes:
        """Fetch the content from the pre-authenticated download url"""
        response = requests.get(self.download_url)
        response.raise_for_status()
        return response.content


class DriveFolderItem(DriveItem):
    """Represents a single M365 folder"""

    def file(self, path: str) -> DriveFileItem:
        """Return a file instance on the given path

        :param path: Path to the file resource
        :return: A DriveFileItem allowing for fetching the content
        :raises: A ValueError if the item is a folder
        """
        response_json = self.graph_client.get(f"{Drive.ENDPOINT}/{self.drive_id}/root:/{path}")
        if "folder" not in response_json:
            return DriveFileItem(
                graph_client=self.graph_client,
                id=response_json["id"],
                drive_id=self.drive_id,
                name=response_json["name"],
                download_url=response_json["@microsoft.graph.downloadUrl"],
            )
        else:
            raise ValueError(f"File requested but item '{path}' is not a file.")

    def glob(self, pattern: str) -> List[DriveFileItem]:
        return []


class Drive(GraphItem):
    ENDPOINT: str = "drives"

    def folder(self, path: str | None = None) -> DriveFolderItem:
        resource_path = f"root:/{path}" if path else "root"
        response_json = self.graph_client.get(f"{self.ENDPOINT}/{self.id}/{resource_path}")
        if "folder" in response_json:
            return DriveFolderItem(
                drive_id=self.id,
                id=response_json["id"],
                graph_client=self.graph_client,
                name=response_json["name"],
            )
        else:
            raise ValueError(f"Folder requested but item '{path}' is not a folder.")
