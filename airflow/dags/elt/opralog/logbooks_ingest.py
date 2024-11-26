"""Ingestion tasks for the Opralog DB logbook tables"""

import datetime as dt
from enum import StrEnum
from pathlib import Path
import os
from s3path import S3Path
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
    LOGBOOK_ENTRIES=dict(unique_keys=("LOGBOOK_ID", "ENTRY_ID"), partition_by=None),
    ENTRIES=dict(unique_keys=["ENTRY_ID"], partition_by=None),
    MORE_ENTRY_COLUMNS=dict(
        unique_keys=["ENTRY_ID", "COLUMN_NO", "ENTRY_TYPE_ID"],
        partition_by=None,
    ),
    ADDITIONAL_COLUMNS=dict(
        unique_keys=["COLUMN_NO", "ENTRY_TYPE_ID"], partition_by=None
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
STAGING_ROOT = os.environ["STAGING_ROOT"]
OPRALOG_DIR = "opralog"
INCOMING_DIR = "incoming"
PARQUET_COMPRESSION = "snappy"
##########


##########
# Utility functions
class SourceLoadType(StrEnum):

    Full = "full"
    Incremental = "incremental"


def write_parquet(
    target: Path, df: pl.DataFrame, partition_by: Optional[Sequence[str]] = None
) -> Path:
    """Write the given DataFrame to the target path as a parquet file/dataset.

    :param filepath: A target path for the file to write
    :param df: A Polars DataFrame
    :param partition_by: An optional list of column names
    """
    if isinstance(target, S3Path):
        fs = S3FileSystem(client_kwargs=S3_STORAGE_OPTIONS)
    else:
        fs = LocalFileSystem(auto_mkdir=False)

    # If a path with the target name exists we remove it. Existing paths can
    # cause issues if for example a path is first stored using partition data in a directory
    # and is then updated to a non-partitioned file. In an S3 bucket both a directory
    # and file with the same name are allowed and this confuses the spark load steps
    # and duplicates the data.
    target_as_str = str(target)  ## the remaining calls require a string path
    if fs.exists(target_as_str):
        fs.rm(
            target_as_str,
            recursive=True,
        )

    if partition_by is not None:
        pq.write_to_dataset(
            df.to_arrow(),
            target_as_str,
            partition_cols=partition_by,
            filesystem=fs,
            compression=PARQUET_COMPRESSION,
        )
    else:
        pq.write_table(
            df.to_arrow(), target_as_str, filesystem=fs, compression=PARQUET_COMPRESSION
        )

    return target


def ensure_dirs(path: Path):
    """If the path is not an s3 path then ensure the directory structure exists"""
    if not isinstance(path, S3Path):
        os.makedirs(path, exist_ok=True)

    return path


def staging_filestem(
    identifier: str, loadtype: SourceLoadType, timestamp: dt.datetime
) -> str:
    """Compute a filename for this source load

    @param identifier An identifier to describe the file content, e.g. source table name
    @param loadtype Full or incremental load type
    @param timestamp Uses the date portion of the timestamp to label the file
    """
    return f"{identifier}_{str(loadtype)}_{timestamp.strftime('%Y%m%d')}"


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

##########
# Tasks


def stage_opralog_tables(
    root_dir: Path, loadtype: SourceLoadType, ingestion_started: dt.datetime
) -> Sequence[Path]:
    """Pull tables from the Opralog source DB and place them in the staging area
    for loading into the Iceberg catalog.

    The source tables will be saved as .parquet files in the following directory structure
    relative to a given root directory:

    root/
    |-- incoming/
    |   |-- [tablename]/
    |       |-- full/
    |       |    |-- YYYY/
    |       |       |-- MM/
    |       |           |-- DD/
    |       |               |-- [tablename]_full_YYYYMMDD.parquet
    |       |-- incremental/
    |           |-- YYYY/
    |               |-- MM/
    |                   |-- DD/
    |                       |-- [tablename]_incremental_YYYYMMDD.parquet

    where the date corresponds to the date of ingestion.
    """
    if loadtype != SourceLoadType.Full:
        raise NotImplementedError("load_type={loadtype} as it is not implemented.")

    staged_paths = []
    for tablename, tableinfo in OPRALOGDB_TABLES.items():
        query = f"SELECT * FROM {tablename}"
        df = pl.read_database_uri(query=query, uri=OPRALOGDB_URI)
        target_path = ensure_dirs(
            staging_path(root_dir, tablename, loadtype, ingestion_started)
        )
        filename = staging_filestem(tablename, loadtype, ingestion_started) + ".parquet"
        staged_paths.append(
            write_parquet(
                target_path / filename, df, partition_by=tableinfo["partition_by"]
            )
        )

    return staged_paths


def transform(staged_paths: Sequence[Path], iceberg_catalog: str, catalog_db: str):
    """Loads the staged parquet files to temporary tables and computes a denormalized
    table holding useful attributes for downstream data products.
    The denormalized table is stored in the Iceberg catalog.

    :param staged_paths: A list of ingested parquet files
    """
    # See logbooks_ingest_transform.pynb
    pass


##########
# Pipeline
##########
staging_dir = (
    S3Path.from_uri(STAGING_ROOT)
    if STAGING_ROOT.startswith("s3:")
    else Path(STAGING_ROOT)
) / OPRALOG_DIR
transform(
    stage_opralog_tables(
        staging_dir, SourceLoadType.Full, dt.datetime.now() - dt.timedelta(days=1)
    )
)
