"""Ingestion tasks for the ISIS running status."""

import datetime as dt
from enum import StrEnum
from pathlib import Path
import os
from s3path import S3Path
from typing import Mapping, Optional, Sequence

from dotenv import load_dotenv
from fsspec.implementations.local import LocalFileSystem
import requests
from s3fs import S3FileSystem

load_dotenv()

##########
# Configuration for external connections
# This should all be moved to some sort of config file.

# Status displau source configuation
RUNNING_STATUS_API = "https://status.isis.stfc.ac.uk/api/schedule"

# S3 Configuration
S3_STORAGE_OPTIONS = {
    "endpoint_url": os.environ["S3_ENDPOINT"],
    "aws_access_key_id": os.environ["S3_ACCESS_KEY"],
    "aws_secret_access_key": os.environ["S3_ACCESS_SECRET"],
    "region_name": os.environ["S3_REGION"],
}

# Task Configuration
STAGING_ROOT = os.environ["STAGING_ROOT"]
STATUSDISPLAY_DIR = "statusdisplay"
INCOMING_DIR = "incoming"
RUNNING_SCHEDULE_ID = "running_schedule"

##########


##########
# Utility functions
# TODO: Shared library???
class SourceLoadType(StrEnum):

    Full = "full"
    Incremental = "incremental"


def ensure_dirs(path: Path):
    """If the path is not an s3 path then ensure the directory structure exists"""
    if not isinstance(path, S3Path):
        os.makedirs(path, exist_ok=True)

    return path


def staging_filestem(
    tablename: str, loadtype: SourceLoadType, timestamp: dt.datetime
) -> str:
    """Compute a stem filename, no extension, for this source load

    @param identifier An identifier to describe the file content, e.g. source table name
    @param loadtype Full or incremental load type
    @param timestamp Uses the date portion of the timestamp to label the file
    """
    return f"{tablename}_{str(loadtype)}_{timestamp.strftime('%Y%m%d')}"


def staging_path(
    root: Path, identifier: str, load_type: SourceLoadType, timestamp: dt.datetime
) -> Path:
    """Compute the path in the storage layer for this source load"""
    return (
        root
        / INCOMING_DIR
        / identifier
        / str(load_type)
        / timestamp.strftime("%Y")
        / timestamp.strftime("%m")
        / timestamp.strftime("%d")
    )


##########
# Tasks


def stage_running_schedule(
    root_dir: Path, loadtype: SourceLoadType, ingestion_started: dt.datetime
) -> Sequence[Path]:
    """Pull the ISIS running schedule from the status display API and save it
    to the staging area as a JSON file under the following structure

    root/
    |-- incoming/
    |   |-- schedule/
    |       |-- full/
    |       |    |-- YYYY/
    |       |       |-- MM/
    |       |           |-- DD/
    |       |               |-- [tablename]_full_YYYYMMDD.parquet

    where the date corresponds to the date of ingestion.
    """
    if loadtype != SourceLoadType.Full:
        raise NotImplementedError("load_type={loadtype} as it is not implemented.")

    target_path = ensure_dirs(
        staging_path(root_dir, RUNNING_SCHEDULE_ID, loadtype, ingestion_started)
    )
    filename = (
        staging_filestem(RUNNING_SCHEDULE_ID, loadtype, ingestion_started) + ".json"
    )
    target = target_path / filename
    s3fs = S3FileSystem(client_kwargs=S3_STORAGE_OPTIONS)
    with s3fs.open(str(target), "w") as s3handle:
        s3handle.write(requests.get(RUNNING_STATUS_API).text)

    return [target]


##########
# Pipeline
##########
staging_dir = (
    S3Path.from_uri(STAGING_ROOT)
    if STAGING_ROOT.startswith("s3:")
    else Path(STAGING_ROOT)
) / STATUSDISPLAY_DIR
stage_running_schedule(
    staging_dir, SourceLoadType.Full, dt.datetime.now() - dt.timedelta(days=1)
)
