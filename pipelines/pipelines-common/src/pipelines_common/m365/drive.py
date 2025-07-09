from dataclasses import dataclass
import fnmatch
from typing import cast, Any, Dict, List

import requests

from .graphapi import GraphItem


def _is_file(graph_response: Dict[str, Any]) -> bool:
    return "file" in graph_response


def _is_folder(graph_response: Dict[str, Any]) -> bool:
    return "folder" in graph_response


class Drive(GraphItem):
    ENDPOINT = "drives"

    def folder(self, path: str | None = None) -> "DriveFolderItem":
        resource_path = f"root:/{path}" if path else "root"
        response_json = self.graph_client.get(f"{self.ENDPOINT}/{self.id}/{resource_path}")
        if _is_folder(response_json):
            return DriveFolderItem(
                drive=self,
                id=response_json["id"],
                graph_client=self.graph_client,
                name=response_json["name"],
            )
        else:
            raise ValueError(f"Folder requested but item '{path}' is not a folder.")


@dataclass
class DriveItem(GraphItem):
    drive: Drive
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

    GLOB_FOLDER_SEP = "/"

    def file(self, path: str) -> DriveFileItem:
        """Return a file instance on the given path

        :param path: Path to the file resource
        :return: A DriveFileItem allowing for fetching the content
        :raises: A ValueError if the item is a folder
        """
        response_json = self.graph_client.get(
            f"{Drive.ENDPOINT}/{self.drive.id}/root:/{path}",
            select=("id", "name", "content.downloadUrl", "file", "folder"),
        )
        if _is_file(response_json):
            return DriveFileItem(
                drive=self.drive,
                graph_client=self.graph_client,
                id=response_json["id"],
                name=response_json["name"],
                download_url=response_json["@microsoft.graph.downloadUrl"],
            )
        else:
            raise ValueError(f"File requested but item '{path}' is not a file.")

    def files_matching(self, pattern: str) -> List[DriveFileItem]:
        """Use this folder as a base and find files that match the glob pattern. Any path segments must be separated with '/' separators.
        This is not recursive.

        :param pattern: A glob pattern, the same as that supplied to the standard glob.glob function.
        :return: A list of [DriveFileItem] objects
        """
        return cast(List[DriveFileItem], self._items_matching(pattern, match_files=True))

    # private
    def _items_matching(
        self, pattern: str, match_files: bool
    ) -> "List[DriveFileItem | DriveFolderItem]":
        """Use this folder as a base, find matching files if match_files=True or folders if match_files=False"""
        children_json = self.graph_client.get(
            f"{self.drive.ENDPOINT}/{self.drive.id}/items/{self.id}/children",
            select=("id", "name", "content.downloadUrl", "file", "folder"),
        )

        items_json = children_json["value"]
        items = []
        for item_json in items_json:
            item_name = item_json["name"]
            if not fnmatch.fnmatchcase(item_name, pattern):
                continue

            if match_files and _is_file(item_json):
                items.append(
                    DriveFileItem(
                        drive=self.drive,
                        graph_client=self.graph_client,
                        id=item_json["id"],
                        name=item_name,
                        download_url=item_json["@microsoft.graph.downloadUrl"],
                    )
                )
            else:
                items.append(
                    DriveFolderItem(
                        drive=self.drive,
                        id=item_json["id"],
                        graph_client=self.graph_client,
                        name=item_json["name"],
                    )
                )

        return items
