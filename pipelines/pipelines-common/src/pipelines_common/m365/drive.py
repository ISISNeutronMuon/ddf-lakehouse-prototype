from dataclasses import dataclass
import fnmatch
from pathlib import PurePosixPath
from typing import cast, Any, Dict, List

import requests

from .graphapi import GraphItem


# Helpers
def _is_file(graph_response: Dict[str, Any]) -> bool:
    return "file" in graph_response


def _is_folder(graph_response: Dict[str, Any]) -> bool:
    return "folder" in graph_response


def _contains_glob_symbols(txt: str) -> bool:
    for char in ("*", "?", "["):
        if char in txt:
            return True

    return False


class Drive(GraphItem):
    ENDPOINT = "drives"
    FILE_SELECT_ATTRS = ("id", "name", "content.downloadUrl", "file", "folder")
    FOLDER_SELECT_ATTRS = ("id", "name", "folder", "file")

    def folder(self, path: str | None = None) -> "DriveFolderItem":
        resource_path = f"root:/{path}" if path else "root"
        response_json = self.graph_client.get(
            f"{self.ENDPOINT}/{self.id}/{resource_path}", select=Drive.FOLDER_SELECT_ATTRS
        )
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

    def files_matching(self, pattern: str) -> List[DriveFileItem]:
        """Use this folder as a base and find files that match the glob pattern. Any path segments must be separated with '/' separators.
        This is not recursive.

        :param pattern: A glob pattern, the same as that supplied to the standard glob.glob function. Glob patterns
                        are limited to globbing a set of directories in the last folder element of the glob, e.g.
                        'a/b/c-*/*.csv' will work but 'a/b-*/c-*/*.csv' will not. The stem is assumed to point to
                        a set of files.
        :return: A list of [DriveFileItem] objects
        """
        if not pattern:
            raise ValueError("Empty pattern supplied.")

        pattern_as_path = PurePosixPath(pattern)
        if len(pattern_as_path.parts) == 1:
            # no folders
            return cast(List[DriveFileItem], self._items_matching(pattern, match_files=True))

        folders_path = pattern_as_path.parent
        if not _contains_glob_symbols(str(pattern_as_path.parent)):
            # folder paths contain no glob syntax
            return self.drive.folder(str(folders_path)).files_matching(pattern_as_path.name)

        raise NotImplementedError("Glob pattern in folders not supported")

        # # is our glob pattern supported?
        # file_stem = pattern_as_path.stem

        # folders = pattern_as_path.parent
        # for part in folders.parts[:-1]:
        #     if _contains_glob_symbols(part):
        #         raise NotImplementedError(
        #             f"Unsupported glob pattern '{pattern}'. Glob patterns are only supported in the file stem and direct parent directory elements of the path."
        #         )

        # first_parent =  pattern_as_path.
        # if len(folders.parts) ==

    # private
    def _items_matching(
        self, pattern: str, match_files: bool
    ) -> "List[DriveFileItem | DriveFolderItem]":
        """Use this folder as a base, find matching files if match_files=True or folders if match_files=False"""
        children_json = self.graph_client.get(
            f"{self.drive.ENDPOINT}/{self.drive.id}/items/{self.id}/children",
            select=Drive.FILE_SELECT_ATTRS,
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
