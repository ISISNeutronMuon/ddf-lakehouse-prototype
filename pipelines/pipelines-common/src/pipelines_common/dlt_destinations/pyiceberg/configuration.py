import dataclasses
from typing import Dict, Literal, Final, Optional, TypeAlias

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
        return {
            key: value for key, value in self.credentials.items() if value is not None
        }
