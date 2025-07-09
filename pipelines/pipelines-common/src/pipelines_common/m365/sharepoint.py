from .drive import Drive
from .graphapi import GraphItem


class Site(GraphItem):
    ENDPOINT: str = "sites"

    def document_library(self) -> Drive:
        response = self.graph_client.get(f"{self.ENDPOINT}/{self.id}/drive", select=["id"])
        return Drive(graph_client=self.graph_client, id=response["id"])
