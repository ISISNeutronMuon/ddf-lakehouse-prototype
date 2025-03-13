"""Utilities for dealing with filesystem-based destinations"""

from typing import cast, Sequence

from dlt.pipeline import Pipeline
from dlt.destinations.impl.filesystem.filesystem import FilesystemClient


def get_table_dirs(pipeline: Pipeline, table_names: Sequence[str]) -> Sequence[str]:
    """Given a pipeline, assuming a filesystem destination, return the path to the given table names"""
    destination_client = cast(FilesystemClient, pipeline.destination_client())
    return [
        destination_client.make_remote_url(destination_client.get_table_dir(table_name))
        for table_name in table_names
    ]
