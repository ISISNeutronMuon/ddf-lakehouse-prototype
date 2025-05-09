import dataclasses
from typing import Any, Dict, Literal, Final, Optional, TypeAlias

from dlt.common.configuration import configspec
from dlt.common.configuration.specs.base_configuration import (
    CredentialsConfiguration,
)
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
        return {"header.X-Iceberg-Access-Delegation": self.access_delegation}


PyIcebergCatalogCredentials: TypeAlias = PyIcebergRestCatalogCredentials


@configspec
class IcebergClientConfiguration(DestinationClientDwhConfiguration):
    destination_type: Final[str] = dataclasses.field(default="pyiceberg", init=False, repr=False, compare=False)  # type: ignore[misc]

    catalog_type: Literal["rest"] = "rest"
    credentials: PyIcebergCatalogCredentials = None  # type: ignore

    def fingerprint(self) -> str:
        """Returns a fingerprint of a connection string."""

        if self.credentials.uri:
            return digest128(self.credentials.uri)
        return ""

    @property
    def connection_properties(self) -> Dict[str, str]:
        """Returns a mapping of connection properties to pass to the catalog constructor"""
        return self.credentials.as_dict()
