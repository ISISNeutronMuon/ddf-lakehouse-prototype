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
        DriveTestSettings.root_api_url(),
        json={"id": "0123456789", "name": "root", "folder": {"childCount": 2}},
    )

    return drive.folder()


def test_drive_init(drive: Drive) -> None:
    assert drive.id == DriveTestSettings.ID


def test_drive_folder_with_no_path_returns_root_folder(drive_root: DriveFolderItem):
    assert isinstance(drive_root, DriveFolderItem)
    assert drive_root.name == "root"


def test_drive_folder_with_path_returns_folder(drive: Drive, requests_mock: RequestsMocker):
    folder = "parent_dir/child_dir"
    requests_mock.get(
        DriveTestSettings.item_api_url(folder),
        json={"id": "0123456789", "name": "child_dir", "folder": {"childCount": 2}},
    )
    folder = drive.folder(folder)

    assert isinstance(folder, DriveFolderItem)
    assert folder.name == "child_dir"


def test_drive_folder_with_path_raises_exception_if_not_a_folder(
    drive: Drive, requests_mock: RequestsMocker
):
    path = "afile"
    requests_mock.get(
        DriveTestSettings.item_api_url(path),
        json={"id": "0123456789", "name": path},
    )

    with pytest.raises(ValueError):
        drive.folder(path)


def test_drive_folder_raises_requests_exception_error_if_path_does_not_exist(
    drive: Drive, requests_mock: RequestsMocker
):
    non_existing_file_path = "NotAFolder"
    requests_mock.get(
        DriveTestSettings.item_api_url(non_existing_file_path),
        status_code=404,
    )

    with pytest.raises(requests.exceptions.HTTPError) as exc:
        drive.folder(non_existing_file_path)

    assert "404" in str(exc)


def test_drivefolderitem_returns_file_item_when_it_exists(
    drive_root: DriveFolderItem, requests_mock: RequestsMocker
) -> None:
    folder, file_name = "Folder", "report.csv"
    file_path = f"{folder}/{file_name}"
    download_url = "https://download.domain.com/468hhnce1047tsh0503856583"
    requests_mock.get(
        DriveTestSettings.item_api_url(file_path),
        json={
            "id": "LKTRFZXCVBOYITUYRHERSG9384",
            "name": file_name,
            "@microsoft.graph.downloadUrl": download_url,
        },
    )

    drive_item = drive_root.file(file_path)

    assert isinstance(drive_item, DriveFileItem)
    assert drive_item.drive.id == DriveTestSettings.ID
    assert drive_item.name == file_name
    assert drive_item.download_url == download_url

    # Success if the download url is available
    file_content = b"colA,colB\n1,2"
    requests_mock.get(download_url, content=file_content)
    assert drive_item.content == file_content

    requests_mock.get(download_url, status_code=401)
    with pytest.raises(requests.exceptions.HTTPError):
        drive_item.content


@pytest.mark.parametrize(
    "file_glob",
    ["*.csv"],
)
def test_drivefolderitem_files_matching_returns_expected_files(
    drive_root: DriveFolderItem, requests_mock: RequestsMocker, file_glob: str
):
    children = DriveTestSettings.mock_requests_for_driveitem_children(drive_root.id, requests_mock)

    matching_files = drive_root.files_matching(file_glob)

    glob_ext = file_glob[-4:]
    expected_file_paths = [
        item["name"] for item in filter(lambda x: x["name"].endswith(glob_ext), children)
    ]
    assert len(matching_files) == len(expected_file_paths)
    for file_item, file_path in zip(matching_files, expected_file_paths):
        assert file_item.name.endswith(glob_ext)
        assert file_item.content == file_path.encode()
