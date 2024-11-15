"""Ingestion tasks for the Opralog DB logbook tables"""

import datetime as dt
from enum import StrEnum
from pathlib import Path
import os
from typing import Mapping, Optional, Sequence

from dotenv import load_dotenv
import polars as pl
import pyarrow.parquet as pq
from fsspec.implementations.local import LocalFileSystem
from s3fs import S3FileSystem

load_dotenv()

##########
# Configuration for external connections
# This should all be moved to some sort of config file.

# Opralog DB Configuation
OPRALOGDB_HOST = os.environ["OPRALOGDB_HOST"]
OPRALOGDB_PORT = os.environ["OPRALOGDB_PORT"]
OPRALOGDB_NAME = os.environ["OPRALOGDB_NAME"]
OPRALOGDB_SCHEMA = os.environ["OPRALOGDB_SCHEMA"]
OPRALOGDB_USER = os.environ["OPRALOGDB_USER"]
OPRALOGDB_PASSWORD = os.environ["OPRALOGDB_PASSWORD"]
OPRALOGDB_URI = (
    f"mssql://"
    f"{OPRALOGDB_USER}:{OPRALOGDB_PASSWORD}@{OPRALOGDB_HOST}:"
    f"{OPRALOGDB_PORT}/{OPRALOGDB_NAME}"
)

# A mapping of table names to information on the table.
# unique_key columnds are used to define an ordering for the table
OPRALOGDB_TABLES: Mapping[str, Optional[Mapping[str, str]]] = dict(
    LOGBOOKS=dict(unique_keys=("LOGBOOK_ID",), partition_by=None),
    LOGBOOK_ENTRIES=dict(
        unique_keys=("LOGBOOK_ID", "ENTRY_ID"), partition_by=["LOGBOOK_ID"]
    ),
    ENTRIES=dict(unique_keys=["ENTRY_ID"], partition_by=None),
    MORE_ENTRY_COLUMNS=dict(
        unique_keys=["ENTRY_ID", "COLUMN_NO", "ENTRY_TYPE_ID"],
        partition_by=["ENTRY_ID"],
    ),
    ADDITIONAL_COLUMNS=dict(
        unique_keys=["COLUMN_NO", "ENTRY_TYPE_ID"], partition_by=["COLUMN_NO"]
    ),
)
OPRALOGDB_LOGBOOKS = ["MCR_RUNNING_LOG"]

# S3 Configuration
S3_STORAGE_OPTIONS = {
    "endpoint_url": os.environ["S3_ENDPOINT"],
    "aws_access_key_id": os.environ["S3_ACCESS_KEY"],
    "aws_secret_access_key": os.environ["S3_ACCESS_SECRET"],
    "region_name": os.environ["S3_REGION"],
}

# Task Configuration
STAGING_ROOT = Path(os.environ["STAGING_ROOT"]) / "opralog"
INCOMING_DIR = "in"
PARQUET_COMPRESSION = "snappy"
##########


##########
# Utility functions
class SourceLoadType(StrEnum):

    Full = "full"
    Incremental = "incremental"


def is_s3_storage(path: Path) -> bool:
    """Return True if path is s3 storage"""
    return str(path).startswith("s3")


def write_parquet(
    target: Path, df: pl.DataFrame, partition_by: Optional[Sequence[str]] = None
):
    """Write the given DataFrame to the target path as a parquet file/dataset

    :param filepath: A target path for the file to write
    :param df: A Polars DataFrame
    :param partition_by: An optional list of column names
    """
    if is_s3_storage(target):
        fs = S3FileSystem(client_kwargs=S3_STORAGE_OPTIONS)
        # target path should not include protocol
        target = "/".join(target.parts[1:])
    else:
        fs = LocalFileSystem(auto_mkdir=False)

    if partition_by is not None:
        pq.write_to_dataset(
            df.to_arrow(),
            target,
            partition_cols=partition_by,
            filesystem=fs,
            compression=PARQUET_COMPRESSION,
        )
    else:
        pq.write_table(
            df.to_arrow(), target, filesystem=fs, compression=PARQUET_COMPRESSION
        )


def ensure_dirs(path: Path):
    """If the path is not an s3 path then ensure the directory structure exists"""
    if not str(path).startswith("s3"):
        os.makedirs(path, exist_ok=True)

    return path


def staging_filename(
    tablename: str, loadtype: SourceLoadType, timestamp: dt.datetime
) -> str:
    """Compute a filename for this source load

    @param tablename The source table name
    @param loadtype Full or incremental load type
    @param timestamp Uses the date portion of the timestamp to label the file
    """
    return f"{tablename}_{str(loadtype)}_{timestamp.strftime('%Y%m%d')}.parquet"


def staging_path(root: Path, load_type: SourceLoadType, timestamp: dt.datetime) -> Path:
    """Compute the path in the storage layer for this source load"""
    return (
        root
        / INCOMING_DIR
        / str(load_type)
        / timestamp.strftime("%Y")
        / timestamp.strftime("%m")
        / timestamp.strftime("%d")
    )


##########

##########
# Tasks


def stage(root_dir: Path, loadtype: SourceLoadType, ingestion_started: dt.datetime):
    """Pull tables from the Opralog source DB and place them in the staging area
    for loading into the Iceberg catalog.

    The source tables will be saved as .parquet files in the following directory structure
    relative to a given root directory:

    root/
    |-- in
    |   |-- full
    |       |-- YYYY/
    |           |-- MM/
    |               |-- DD/
    |                   |-- tablename_full_YYYYMMDD.parquet
    |   |-- incremental
    |       |-- YYYY/
    |           |-- MM/
    |               |-- DD/
    |                   |-- tablename_incremental_YYYYMMDD.parquet

    where the date corresponds to the date of ingestion.
    """
    if loadtype != SourceLoadType.Full:
        raise NotImplementedError("load_type={loadtype} as it is not implemented.")

    for tablename, tableinfo in OPRALOGDB_TABLES.items():
        unique_keys = tableinfo["unique_keys"]
        query = f"SELECT TOP 100 * FROM {tablename} ORDER BY {','.join(unique_keys)}"
        df = pl.read_database_uri(query=query, uri=OPRALOGDB_URI)
        target_path = ensure_dirs(staging_path(root_dir, loadtype, ingestion_started))
        filename = staging_filename(tablename, loadtype, ingestion_started)
        write_parquet(
            target_path / filename, df, partition_by=tableinfo["partition_by"]
        )


stage(STAGING_ROOT, SourceLoadType.Full, dt.datetime.now())
