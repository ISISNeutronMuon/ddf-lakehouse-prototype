from .drive import Drive
from .graphapi import GraphClientV1, GraphItem


class Site(GraphItem):
    ENDPOINT: str = "sites"

    def document_library(self) -> Drive:
        response = self.graph_client.get(f"{self.ENDPOINT}/{self.id}/drive")
        return Drive(graph_client=self.graph_client, id=response["id"])
