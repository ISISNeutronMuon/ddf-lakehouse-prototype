import os
from typing import List

# patch which providers to enable
from dlt.common.configuration.providers import (
    ConfigProvider,
    EnvironProvider,
    SecretsTomlProvider,
    ConfigTomlProvider,
)
from dlt.common.runtime.run_context import RunContext


def initial_providers(self) -> List[ConfigProvider]:
    # do not read the global config
    # find the .dlt in the same directory as this file
    return [
        EnvironProvider(),
        SecretsTomlProvider(settings_dir="e2e_tests/.dlt"),
        ConfigTomlProvider(settings_dir="e2e_tests/.dlt"),
    ]


RunContext.initial_providers = initial_providers  # type: ignore[method-assign]
