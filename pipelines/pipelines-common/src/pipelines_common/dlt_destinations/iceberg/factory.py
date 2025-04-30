import typing as t

from dlt.common.destination import Destination, DestinationCapabilitiesContext

from pipelines_common.dlt_destinations.iceberg.configuration import (
    IcebergClientConfiguration,
    IcebergCatalogCredentials,
)


class iceberg(Destination[IcebergClientConfiguration, "IcebergClient"]):
    spec = IcebergClientConfiguration

    def _raw_capabilities(self) -> DestinationCapabilitiesContext:
        caps = DestinationCapabilitiesContext()
        caps.preferred_loader_file_format = "parquet"
        caps.supported_loader_file_formats = ["parquet"]
        caps.preferred_table_format = "iceberg"
        caps.supported_table_formats = ["iceberg"]
        caps.has_case_sensitive_identifiers = True

        caps.max_identifier_length = 255
        caps.max_column_identifier_length = 255

        caps.supported_merge_strategies = ["delete-insert"]
        caps.supported_replace_strategies = [
            "truncate-and-insert",
        ]
        return caps

    @property
    def client_class(self) -> t.Type["IcebergClient"]:
        from pipelines_common.dlt_destinations.iceberg.iceberg import (
            IcebergClient,
        )

        return IcebergClient

    def __init__(
        self,
        credentials: IcebergCatalogCredentials = None,
        **kwargs: t.Any,
    ) -> None:
        """Configure the Iceberg destination to use in a pipeline.

        All arguments provided here supersede other configuration sources such as environment variables and dlt config files.

        Args:
            credentials: An instance of IcebergCatalogCredentials to connect to the iceberg catalog.
        """
        super().__init__(
            credentials=credentials,
            **kwargs,
        )


iceberg.register()
