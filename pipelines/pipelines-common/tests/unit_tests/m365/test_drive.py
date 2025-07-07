from pipelines_common.m365.drive import Drive, DriveFileItem, DriveFolderItem
from pipelines_common.m365.graphapi import GraphClientV1

import pytest
import requests
from requests_mock.mocker import Mocker as RequestsMocker

from unit_tests.m365.conftest import DriveTestSettings


@pytest.fixture
def drive(graph_client: GraphClientV1) -> Drive:
    return Drive(graph_client=graph_client, id=DriveTestSettings.ID)


@pytest.fixture
def drive_root(drive: Drive, requests_mock: RequestsMocker) -> DriveFolderItem:
    requests_mock.get(
        DriveTestSettings.root_api_url(drive.graph_client),
        json={"id": "0123456789", "name": "root"},
    )
    return drive.root()


def test_drive_init(drive: Drive) -> None:
    assert drive.id == DriveTestSettings.ID


def test_drive_root_has_expected_name(drive_root: DriveFolderItem, requests_mock: RequestsMocker):
    assert drive_root.name == "root"


def test_drive_folder_get_file_raises_requests_exception_error_if_path_does_not_exist(
    drive_root: DriveFolderItem, requests_mock: RequestsMocker
):
    non_existing_file_path = "NotAFolder/NotAFile.txt"
    requests_mock.get(
        DriveTestSettings.item_api_url(drive_root.graph_client, non_existing_file_path),
        exc=requests.exceptions.HTTPError("404"),
    )

    with pytest.raises(requests.exceptions.HTTPError) as exc:
        drive_root.get_file(non_existing_file_path)

    assert "404" in str(exc)


def test_drive_folder_get_file_raises_value_error_if_path_exists_but_is_folder(
    drive_root: DriveFolderItem, requests_mock: RequestsMocker
):
    folder_path = "FolderA"
    requests_mock.get(
        DriveTestSettings.item_api_url(drive_root.graph_client, folder_path),
        json={"name": folder_path, "folder": {"childCount": 2}},
    )

    with pytest.raises(ValueError) as exc:
        drive_root.get_file(folder_path)

    assert "but is a folder" in str(exc)


# def test_drive_folder_glob_returns_expected_files(
#     drive_root: DriveFolderItem, requests_mock: RequestsMocker
# ):
#     csv_files = drive_root.glob("*.csv")

#     for file_item in csv_files:
#         assert file_item.name.endswith(".csv")


# def test_fetch_item_content_raises_exception_for_non_existant_file(
#     m365_drive: M365Drive, requests_mock: RequestsMocker
# ):
#     requests_mock.get(
#         SharePointTestSettings.site_library_item_api_url(
#             m365_drive.graph_client, non_existing_file_path
#         ),
#         exc=requests.exceptions.HTTPError("404"),
#     )

#     with pytest.raises(requests.exceptions.HTTPError) as excinfo:
#         m365_drive.fetch_item_content(non_existing_file_path)

#     assert "404" in str(excinfo.value)


# def test_fetch_item_content_retrieves_content_of_existing_file_from_download_url(
#     m365_drive: M365Drive, requests_mock: RequestsMocker
# ):
#     existing_file_path, existing_file_content = "SubFolder/AFile.txt", b"some test file content"
#     test_downloadurl = "https://test.download.com/34534efwef54"
#     requests_mock.get(
#         SharePointTestSettings.site_library_item_api_url(
#             m365_drive.graph_client, existing_file_path
#         ),
#         json={GraphClientV1.Key.DOWNLOADURL: test_downloadurl},
#     )
#     requests_mock.get(test_downloadurl, content=existing_file_content)

#     fetched_file_content = m365_drive.fetch_item_content(existing_file_path)

#     assert fetched_file_content != fetched_file_content.getvalue()
