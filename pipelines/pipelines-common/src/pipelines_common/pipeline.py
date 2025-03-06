import os

_PIPELINE_ENV_VAR = "PIPELINE_ENV"
_PIPELINE_ENV_NAME_PROD = "prod"
_PIPELINE_ENV_NAME_DEV = "dev"


def _pipeline_environment():
    """The configured pipeline name. Defaults to dev"""
    return os.environ.get(_PIPELINE_ENV_VAR, _PIPELINE_ENV_NAME_DEV)


def _is_production() -> bool:
    """Are we running in a production environment based on the
    value of the configured environment variable."""
    return os.environ.get(_PIPELINE_ENV_VAR, "") == _PIPELINE_ENV_NAME_PROD


def _pipeline_prefix() -> str:
    """Return the prefix for the pipeline name in this environment"""
    return "" if _is_production() else f"{_pipeline_environment()}_"


def pipeline_name(basename: str) -> str:
    """Given an unqualified pipeline name,
    optionally prefix it depending on the PIPELINE_ENV environment variable

    :param basename: The unqualified name for the pipeline
    """
    return f"{_pipeline_prefix()}{basename}"
