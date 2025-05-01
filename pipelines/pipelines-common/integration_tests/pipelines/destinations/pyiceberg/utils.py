from dataclasses import dataclass
import os
from typing import cast, Any, Dict, Optional, Union

import dlt
from dlt.common.configuration.specs import CredentialsConfiguration
from dlt.common.destination import (
    TDestinationReferenceArg,
)
from dlt.common.destination.exceptions import SqlClientNotAvailable

from pipelines_common.dlt_destinations.pyiceberg.pyiceberg import PyIcebergClient


@dataclass
class PyIcebergDestinationTestConfiguration:
    """Class for defining test setup for pyiceberg destination."""

    destination: TDestinationReferenceArg = (
        "pipelines_common.dlt_destinations.pyiceberg"
    )
    credentials: Optional[Union[CredentialsConfiguration, Dict[str, Any], str]] = None
    env_vars: Optional[Dict[str, str]] = None

    def setup(self) -> None:
        """Sets up environment variables for this destination configuration"""
        if self.credentials is not None:
            if isinstance(self.credentials, str):
                os.environ["DESTINATION__CREDENTIALS"] = self.credentials
            else:
                for key, value in dict(self.credentials).items():
                    os.environ[f"DESTINATION__CREDENTIALS__{key.upper()}"] = str(value)

        if self.env_vars is not None:
            for k, v in self.env_vars.items():
                os.environ[k] = v

    def setup_pipeline(
        self,
        pipeline_name: str,
        dataset_name: str = None,
        dev_mode: bool = False,
        **kwargs,
    ) -> dlt.Pipeline:
        """Convenience method to setup pipeline with this configuration"""
        self.setup()
        self.active_pipeline = dlt.pipeline(
            pipeline_name=pipeline_name,
            pipelines_dir=kwargs.pop("pipelines_dir", None),
            destination=self.destination,
            dataset_name=(
                dataset_name if dataset_name is not None else pipeline_name + "_data"
            ),
            dev_mode=dev_mode,
            **kwargs,
        )
        return cast(dlt.Pipeline, self.active_pipeline)

    def clean_catalog(self):
        """Clean the destination catalog of all namespaces and tables"""
        pipeline = self.active_pipeline
        if pipeline is None:
            return

        with pipeline.destination_client() as client:
            catalog = cast(PyIcebergClient, client).iceberg_catalog
            for ns_name in catalog.list_namespaces():
                tables = catalog.list_tables(ns_name)
                for qualified_table_name in tables:
                    catalog.purge_table(qualified_table_name)

    def attach_pipeline(self, pipeline_name: str, **kwargs) -> dlt.Pipeline:
        """Attach to existing pipeline keeping the dev_mode"""
        # remember dev_mode from setup_pipeline
        pipeline = dlt.attach(pipeline_name, **kwargs)
        return pipeline

    def supports_sql_client(self, pipeline: dlt.Pipeline) -> bool:
        """Checks if destination supports SQL queries"""
        try:
            pipeline.sql_client()
            return True
        except SqlClientNotAvailable:
            return False

    @property
    def active_pipeline(self) -> dlt.Pipeline | None:
        return self._active_pipeline

    @active_pipeline.setter
    def active_pipeline(self, value: dlt.Pipeline):
        self._active_pipeline = value
