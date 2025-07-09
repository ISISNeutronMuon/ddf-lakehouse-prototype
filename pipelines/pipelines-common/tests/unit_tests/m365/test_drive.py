from pipelines_common.m365.drive import Drive, DriveFolderItem
from pipelines_common.m365.graphapi import GraphClientV1

import pytest
import requests
from requests_mock.mocker import Mocker as RequestsMocker

from unit_tests.m365.conftest import DriveTestSettings, drive_glob_test_cases


@pytest.fixture
def drive(graph_client: GraphClientV1) -> Drive:
    return Drive(graph_client=graph_client, id=DriveTestSettings.ID)


@pytest.fixture
def drive_root(drive: Drive, requests_mock: RequestsMocker) -> DriveFolderItem:
    requests_mock.get(
        DriveTestSettings.root_api_url(),
        json={"id": "0123456789", "name": "root", "folder": {}},
        complete_qs=True,
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
        DriveTestSettings.item_api_url(folder, is_file=False),
        json={"id": "0123456789", "name": "child_dir", "folder": {}},
        complete_qs=True,
    )
    folder = drive.folder(folder)

    assert isinstance(folder, DriveFolderItem)
    assert folder.name == "child_dir"


def test_drive_folder_with_path_raises_exception_if_not_a_folder(
    drive: Drive, requests_mock: RequestsMocker
):
    path = "afile"
    # request should look like we are requesting a folder but we get a file back
    requests_mock.get(
        DriveTestSettings.item_api_url(path, is_file=False),
        json={"id": "0123456789", "name": path, "file": {}},
        complete_qs=True,
    )

    with pytest.raises(ValueError):
        drive.folder(path)


def test_drive_folder_raises_requests_exception_error_if_path_does_not_exist(
    drive: Drive, requests_mock: RequestsMocker
):
    non_existing_file_path = "NotAFolder"
    requests_mock.get(
        DriveTestSettings.item_api_url(non_existing_file_path, is_file=False), status_code=404
    )

    with pytest.raises(requests.exceptions.HTTPError) as exc:
        drive.folder(non_existing_file_path)

    assert "404" in str(exc)


def test_drivefolderitem_files_matching_raises_valueerror_if_pattern_empty(
    drive_root: DriveFolderItem,
):
    with pytest.raises(ValueError):
        drive_root.files_matching("")


def test_drivefolderitem_files_matching_raises_notimplementederror_if_pattern_not_supported(
    drive_root: DriveFolderItem,
):
    with pytest.raises(NotImplementedError):
        drive_root.files_matching("subdir-*/subsubdir/file.csv")


@pytest.mark.parametrize(
    "glob_test_cases",
    drive_glob_test_cases(),
    ids=lambda x: x.name,
)
def test_drivefolderitem_files_matching_returns_expected_files(
    drive_root: DriveFolderItem,
    requests_mock: RequestsMocker,
    glob_test_cases: DriveTestSettings,
):
    expected_file_paths = glob_test_cases.mock_requests_for_driveitem_children(
        requests_mock, drive_root.id
    )

    matching_files = drive_root.files_matching(glob_test_cases.glob_pattern)

    assert len(matching_files) == len(expected_file_paths)
    for file_item, file_path in zip(matching_files, expected_file_paths):
        assert file_item.name == file_path
        assert file_item.content == file_path.encode()
