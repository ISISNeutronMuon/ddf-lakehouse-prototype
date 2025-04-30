from dataclasses import dataclass
import os
from typing import Any, Dict, Optional, Union

import dlt
from dlt.common.configuration.specs import CredentialsConfiguration
import pytest


@dataclass
class DestinationIcebergTestConfiguration:
    destination: str
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

        self.dev_mode = dev_mode
        self.setup()
        pipeline = dlt.pipeline(
            pipeline_name=pipeline_name,
            destination=self.destination,
            dataset_name=(
                dataset_name if dataset_name is not None else pipeline_name + "_data"
            ),
            dev_mode=dev_mode,
            **kwargs,
        )
        return pipeline
