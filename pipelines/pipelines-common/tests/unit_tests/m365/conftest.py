import itertools
from pathlib import Path
from typing import List

import pytest
from pytest_mock import MockerFixture
from requests_mock.mocker import Mocker as RequestsMocker
from pytest_httpx import HTTPXMock

from pipelines_common.m365.drive import Drive
from pipelines_common.m365.graphapi import (
    GraphCredentials,
    GraphClientV1,
)


class MSGraphTestSettings:
    TENANT_ID: str = "tenant1"
    CLIENT_ID: str = "client1"
    CLIENT_SECRET: str = "clients3cr3t"
    ACCESS_TOKEN: str = "0987654321abcdefg"


class SharePointTestSettings:
    HOSTNAME: str = "site.domain.com"
    SITE_PATH: str = "sites/MySite"
    SITE_ID: str = (
        f"{HOSTNAME},0000000-1111-2222-3333-444444444444,55555555-6666-7777-8888-99999999999"
    )
    LIBRARY_ID: str = "b!umnFmiec4Blh4I3Cv5be8uPs4IEW9cGoyL2iMLaafz2yjlrRtGbxqbR31mJiv5hCZfL"

    @classmethod
    def site_api_url(cls) -> str:
        return f"{GraphClientV1.api_url}/sites/{cls.HOSTNAME}:/{cls.SITE_PATH}"

    @classmethod
    def site_library_api_url(cls, *, select=None) -> str:
        select = select if select is not None else ["id"]
        return f"{GraphClientV1.api_url}/sites/{SharePointTestSettings.SITE_ID}/drive?$select={','.join(select)}"

    @classmethod
    def mock_request_to_site_library(cls, requests_mock: RequestsMocker):
        requests_mock.get(cls.site_api_url(), json={"id": cls.SITE_ID})

    # def mock_requests_for_file(
    #     cls, requests_mock: RequestsMocker, file_path: str, file_content: bytes
    # ):
    #     requests_mock.get(cls.site_api_url(), json={"id": cls.SITE_ID})
    #     requests_mock.get(cls.site_library_api_url(), json={"id": cls.LIBRARY_ID})
    #     requests_mock.get(
    #         f"{GraphClientV1.api_url}/drives/{cls.LIBRARY_ID}/root",
    #         json={"id": cls.LIBRARY_ID, "name": "root"},
    #     )
    #     download_url = "https://file.download.url/458972349iuf"
    #     requests_mock.get(
    #         f"{GraphClientV1.api_url}/drives/{cls.LIBRARY_ID}/root:/{file_path}",
    #         json={
    #             "id": cls.LIBRARY_ID,
    #             "name": Path(file_path).name,
    #             "@microsoft.graph.downloadUrl": download_url,
    #         },
    #     )
    #     requests_mock.get(download_url, content=file_content)


class DriveTestSettings:
    ID: str = "b!ejiYvIQW9PJmAu7cuils0N8UUvV7zURwlBMY4DJi1NRD58OMbRFXjb16RGKIn5ujQ"
    DOWNLOAD_BASE_URL: str = "https://example.file.download.url"

    @classmethod
    def root_api_url(cls) -> str:
        return f"{GraphClientV1.api_url}/drives/{cls.ID}/root?$select={','.join(Drive.FOLDER_SELECT_ATTRS)}"

    @classmethod
    def item_api_url(cls, path: str, is_file: bool) -> str:
        select = Drive.FILE_SELECT_ATTRS if is_file else Drive.FOLDER_SELECT_ATTRS
        return f"{GraphClientV1.api_url}/drives/{cls.ID}/root:/{path}?$select={','.join(select)}"

    def __init__(self, glob_pattern: str, expected_matching_files: List[str]) -> None:
        self.glob_pattern = glob_pattern
        self.expected_matching_files = expected_matching_files

    @property
    def name(self) -> str:
        return self.glob_pattern

    def mock_requests_for_driveitem_children(
        self, requests_mock: RequestsMocker, root_id: str
    ) -> List[str]:
        # We create a fake directory structure:
        #  - subdir-0/
        #    |- subsubdir-0/
        #       |- subsubdir-0-0.csv
        #       |- subsubdir-0-1.csv
        #       |- subsubdir-0-0.docx
        #       |- subsubdir-0-1.docx
        #    |- subdir-0-0.csv
        #    |- subdir-0-1.csv
        #    |- subdir-0-0.docx
        #    |- subdir-0-1.docx
        #  - subdir-1/
        #    |- subdir-1-0.csv
        #    |- subdir-1-1.csv
        #    |- subdir-1-0.docx
        #    |- subdir-1-1.docx
        #  - root-0.csv
        #  - root-1.csv
        #  - root-1.docx
        #  - root-1.docx
        def create_dirs_json(prefix: str, num: int):
            return [
                {"id": f"{prefix}-{i}", "name": f"{prefix}-{i}", "folder": {}} for i in range(num)
            ]

        def create_files_json(requests_mock: RequestsMocker, prefix: str, ext: str, num: int):
            resp = []
            for i in range(num):
                name = f"{prefix}-{i}{ext}"
                download_url = f"{self.DOWNLOAD_BASE_URL}/{name}"
                requests_mock.get(download_url, content=name.encode())
                resp.append(
                    {
                        "id": name,
                        "name": name,
                        "file": {},
                        "@microsoft.graph.downloadUrl": download_url,
                    }
                )
            return resp

        # 2 top-level directories & 4 top-level files
        requests_mock.get(
            f"{GraphClientV1.api_url}/drives/{self.ID}/items/{root_id}/children",
            json={
                "value": list(
                    itertools.chain(
                        create_dirs_json("subdir", num=1),
                        create_files_json(requests_mock, "root", ".csv", num=2),
                        create_files_json(requests_mock, "root", ".docx", num=2),
                    )
                )
            },
        )
        # subdir-{0,1} node & children
        for subdir_index in range(2):
            subdir_name = f"subdir-{subdir_index}"
            requests_mock.get(
                f"{GraphClientV1.api_url}/drives/{self.ID}/root:/{subdir_name}",
                json=create_dirs_json("subdir", num=1)[0],
            )
            requests_mock.get(
                f"{GraphClientV1.api_url}/drives/{self.ID}/items/{subdir_name}/children",
                json={
                    "value": list(
                        itertools.chain(
                            create_dirs_json("subsubdir", num=2),
                            create_files_json(requests_mock, subdir_name, ".csv", num=2),
                            create_files_json(requests_mock, subdir_name, ".docx", num=2),
                        )
                    )
                },
            )

        return self.expected_matching_files


def drive_glob_test_cases() -> List[DriveTestSettings]:
    return [
        DriveTestSettings(glob_pattern="root-0.csv", expected_matching_files=["root-0.csv"]),
        # DriveTestSettings(
        #     glob_pattern="*.csv",
        #     expected_matching_files=[f"root-{index}.csv" for index in range(2)],
        # ),
        # DriveTestSettings(
        #     glob_pattern="subdir-0/*.docx",
        #     expected_matching_files=[f"subdir-0-{index}.docx" for index in range(2)],
        # ),
        # DriveTestSettings(
        #     glob_pattern="subdir-*/*.docx",
        #     expected_matching_files=(
        #         [f"subdir-{dirindex}-{index}.docx" for dirindex in range(2) for index in range(2)]
        #     ),
        # ),
    ]


@pytest.fixture
def graph_client(mocker: MockerFixture, httpx_mock: HTTPXMock) -> GraphClientV1:
    patch_outh_client_with_access_token(mocker)

    client = GraphClientV1(
        GraphCredentials(
            MSGraphTestSettings.TENANT_ID,
            MSGraphTestSettings.CLIENT_ID,
            MSGraphTestSettings.CLIENT_SECRET,
        )
    )
    httpx_mock.add_response(
        method="GET",
        url=client.openid_config_url,
        json={"token_endpoint": "https://token.endpoint"},
    )
    return client


def patch_outh_client_with_access_token(mocker: MockerFixture):
    return patch_oauth_client_to_return({"access_token": MSGraphTestSettings.ACCESS_TOKEN}, mocker)


def patch_oauth_client_to_return(msal_response: dict, mocker: MockerFixture):
    patched_client_app = mocker.patch("pipelines_common.m365.graphapi.OAuth2Client")
    patched_client_app.return_value.fetch_token.return_value = msal_response

    return patched_client_app
