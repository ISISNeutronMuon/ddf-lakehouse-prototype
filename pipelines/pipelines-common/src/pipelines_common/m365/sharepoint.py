from msgraphfs import MSGDriveFS

from .graphapi import GraphItem


class Site(GraphItem):
    endpoint: str = "sites"

    def document_library(self) -> MSGDriveFS:
        response = self.graph_client.get(f"{self.endpoint}/{self.id}/drive", select=["id"])
        drivefs_kwargs = {
            "drive_id": response["id"],
            "oauth2_client_params": self.graph_client.oauth2_client_params(),
            "asynchronous": False,
            "cachable": False,
        }
        return MSGDriveFS(**drivefs_kwargs)
