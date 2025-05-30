# import os
# from typing import cast, Optional

import dlt
from dlt import Pipeline

# from dlt.common.configuration.resolve import inject_section
# from dlt.common.configuration.specs import ConfigSectionContext
# from dlt.common.libs.pandas import DataFrame
from dlt.pipeline.progress import TCollectorArg, _NULL_COLLECTOR as NULL_COLLECTOR

from pipelines_common.constants import DATASET_NAME_PREFIX_SRCS


def source_dataset_name(suffix: str) -> str:
    """Given a suffix return the full source dataset name"""
    return f"{DATASET_NAME_PREFIX_SRCS}{suffix}"


def create_pipeline(
    name: str,
    destination: str,
    dataset_name: str | None = None,
    progress: TCollectorArg = NULL_COLLECTOR,
) -> Pipeline:
    """Create a named pipeline object for a given destination.

    If a dataset_name is not provided then the dataset name is set to src_{pipeline_name}
    """
    return dlt.pipeline(
        pipeline_name=name,
        dataset_name=(dataset_name if dataset_name is not None else source_dataset_name(name)),
        destination=destination,
        progress=progress,
    )
