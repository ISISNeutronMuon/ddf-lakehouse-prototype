import dataclasses
from typing import Dict, Literal, Final, Optional, TypeAlias

from dlt.common.configuration import configspec
from dlt.common.configuration.specs import CredentialsConfiguration
from dlt.common.destination.client import DestinationClientDwhConfiguration
from dlt.common.utils import digest128


@configspec(init=False)
class PyIcebergRestCatalogCredentials(CredentialsConfiguration):
    uri: str = None  # type: ignore
    warehouse: Optional[str] = None
    access_delegation: Literal["vended-credentials", "remote-signing"] = (
        "vended-credentials"
    )

    def as_dict(self) -> Dict[str, str]:
        """Return the credentials as a dictionary suitable for the Catalog constructor"""
        properties: Dict[str, str] = self.headers_as_properties()
        for key, value in self.items():
            if value is None or key == "access_delegation":
                continue
            properties[key] = value

        return properties

    def headers_as_properties(self) -> Dict[str, str]:
        from pyiceberg.utils.properties import HEADER_PREFIX

        return {
            f"{HEADER_PREFIX}X-Iceberg-Access-Delegation": self.access_delegation,
        }


PyIcebergCatalogCredentials: TypeAlias = PyIcebergRestCatalogCredentials


@configspec(init=False)
class IcebergClientConfiguration(DestinationClientDwhConfiguration):
    destination_type: Final[str] = dataclasses.field(default="pyiceberg", init=False, repr=False, compare=False)  # type: ignore[misc]

    bucket_url: str = None  # type: ignore
    catalog_type: Literal["rest"] = "rest"
    credentials: PyIcebergCatalogCredentials = None  # type: ignore

    # possible placeholders: {dataset_name}, {table_name}, {location_tag}
    # This is deliberately prefixed with an 'r' to indicate it is not an f-string
    # and is intended to be expanded later
    table_location_layout: Optional[str] = r"{dataset_name}/{table_name}"

    @property
    def connection_properties(self) -> Dict[str, str]:
        """Returns a mapping of connection properties to pass to the catalog constructor"""
        return self.credentials.as_dict() if self.credentials is not None else {}

    def on_resolved(self) -> None:
        self.normalize_bucket_url()

    def normalize_bucket_url(self) -> None:
        """Normalizes bucket_url, i.e. removes any trailing slashes"""
        self.bucket_url = self.bucket_url.rstrip("/")

    def fingerprint(self) -> str:
        """Returns a fingerprint of a connection string."""

        if self.credentials.uri:
            return digest128(self.credentials.uri)
        return ""
