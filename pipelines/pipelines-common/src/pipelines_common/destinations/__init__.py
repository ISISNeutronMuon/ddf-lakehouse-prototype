"""General utilities for dealing with destinations"""

from dlt import Pipeline


def raise_if_destination_not(expected: str, pipeline: Pipeline):
    if pipeline.destination.destination_name != expected:
        raise NotImplementedError(
            f"Expected destination_type={expected}, found={pipeline.destination.destination_name}"
        )
