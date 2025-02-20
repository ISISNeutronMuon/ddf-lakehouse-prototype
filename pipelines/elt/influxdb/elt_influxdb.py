import argparse
from collections.abc import Generator
import datetime as dt
import logging
import os
from typing import Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import dlt
from dlt.extract import DltResource
import dlt.common.normalizers.naming.snake_case as snake_case
import humanize
import pandas as pd
import requests

from pyiceberg.transforms import YEAR

from pipelines_common.constants import (
    MICROSECONDS_PER_SEC,
    MICROSECONDS_STR,
    MICROSECONDS_STR_INFLUX,
    SOURCE_NAMESPACE_PREFIX,
    SOURCE_TABLE_PREFIX,
)
from pipelines_common.destinations.pyiceberg import pyiceberg
from pipelines_common.utils.iceberg import Catalog, catalog_create

LOGGER = logging.getLogger(__name__)

DEVEL = os.environ.get("DEVEL", "true") == "true"
PIPELINE_NAME = f"{"dev_" if DEVEL else ""}elt_influxdb"
NAMESPACE_NAME = f"{SOURCE_NAMESPACE_PREFIX}influxdb"

# Source
INFLUXDB_MACHINESTATE_BUCKET = "machinestate"
INFLUXDB_MACHINESTATE_TIMES = (
    dt.datetime(2018, 1, 1, tzinfo=dt.timezone.utc),
    dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc),
)

# Destination
COLUMN_CHANNEL_NAME = "channel_name"
PARTITION_HINT = "x-partition-spec"


def to_utc_str(timestamp: dt.datetime) -> str:
    return timestamp.astimezone(ZoneInfo("UTC")).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def measurement_channel_table_name(
    bucket_name: str, channel_name: str, normalized: bool = False
):
    """Compute the table name for a given channel in a bucket with optional normalization"""
    unnormalized_name = f"{SOURCE_TABLE_PREFIX}{bucket_name}_{channel_name}"
    return (
        snake_case.NamingConvention().normalize_identifier(unnormalized_name)
        if normalized
        else unnormalized_name
    )


def measurement_channel_last_times(
    pipeline_name: str, namespace_name: str, bucket_name: str, channels: List[str]
):
    last_loaded_times = {}
    destination_catalog = catalog_create(
        dlt.config[f"{pipeline_name}.destination.pyiceberg.catalog_properties"]
    )
    for channel_name in channels:
        table_id = f"{namespace_name}.{measurement_channel_table_name(bucket_name, channel_name, normalized=True)}"
        last_time_value = destination_last_time_value(destination_catalog, table_id)
        if last_time_value is not None:
            last_loaded_times[channel_name] = last_time_value

    return last_loaded_times


def measurement_list_table_name(bucket_name):
    return f"{SOURCE_TABLE_PREFIX}{bucket_name}_measurement_name"


def influxdb_channel_names(
    bucket_name: str, base_url: str, auth_token: str
) -> List[str]:
    def flatten(nested):
        return [subitem for item in nested for subitem in item]

    resp = requests.get(
        f"{base_url}/query",
        headers={"Authorization": f"Token {auth_token}"},
        params={"db": bucket_name, "q": f"SHOW MEASUREMENTS"},
    )
    resp_json = resp.json()
    try:
        series = resp_json["results"][0]["series"][0]
        return flatten(series["values"])
    except (IndexError, KeyError):
        raise ValueError(
            f"Error querying Influx for available channel names. Unexpected result structure:\n '{resp_json}'"
        )


def influxdb_get_channel_start(
    bucket_name: str,
    channel_name: str,
    base_url: str,
    auth_token: str,
) -> Optional[dt.datetime]:
    """Return the timestamp of the first recorded value for the given bucket and channel.

    None is returned if a value cannot be determined
    """
    query = f'SELECT "value" FROM /^{channel_name}$/ LIMIT 1'
    resp = requests.get(
        f"{base_url}/query",
        headers={"Authorization": f"Token {auth_token}"},
        params={"db": bucket_name, "epoch": MICROSECONDS_STR_INFLUX, "q": query},
    )
    resp_json = resp.json()
    try:
        return dt.datetime.fromtimestamp(
            resp_json["results"][0]["series"][0]["values"][0][0] / MICROSECONDS_PER_SEC
        )
    except (IndexError, KeyError):
        return None


def destination_last_time_value(
    catalog: Catalog, table_id: str
) -> Optional[dt.datetime]:
    """Peek into the loaded table and return the timestamp of the latest piece of data"""
    if catalog.table_exists(table_id):
        table = catalog.load_table(table_id)
        last_value = (
            table.scan(selected_fields=("time",)).to_arrow().sort_by("time")["time"][-1]
        )
        return dt.datetime.fromtimestamp(
            last_value.value / MICROSECONDS_PER_SEC, tz=dt.timezone.utc
        )
    else:
        return None


@dlt.resource()
def influxdb_get_measurement(
    bucket_name: str,
    channel_name: str,
    time: dlt.sources.incremental = dlt.sources.incremental("time"),
    base_url: str = dlt.config.value,
    auth_token: str = dlt.secrets.value,
):
    """Load all data for a given channel, yielding in yearly chunks"""
    time_start = time.start_value
    time_end = (
        time.end_value
        if time.end_value is not None
        else time.start_value + dt.datetime.now()
    )

    chunk_start, chunk_end = time_start, time_start + dt.timedelta(weeks=52)
    while chunk_start < chunk_end:
        query_t0, query_t1 = to_utc_str(chunk_start), to_utc_str(chunk_end)
        query = f"SELECT \"value\" FROM /^{channel_name}$/ WHERE time >= '{query_t0}' AND time < '{query_t1}'"
        LOGGER.debug(query)
        resp = requests.get(
            f"{base_url}/query",
            headers={"Authorization": f"Token {auth_token}"},
            params={"db": bucket_name, "epoch": MICROSECONDS_STR_INFLUX, "q": query},
        )
        resp_json = resp.json()
        for all_series in resp_json["results"]:
            # Do we have any results for this time regime
            if "series" not in all_series:
                break
            for series in all_series["series"]:
                df = pd.DataFrame.from_records(
                    series["values"], columns=series["columns"]
                )
                df["time"] = pd.to_datetime(
                    df["time"], origin="unix", unit=MICROSECONDS_STR
                )
                df["value"] = df["value"].astype("float64")
                df[COLUMN_CHANNEL_NAME] = channel_name
                yield df
        # next chunk
        chunk_start = chunk_end
        chunk_end = min(chunk_start + dt.timedelta(weeks=52), time_end)


# machinestate pipeline
@dlt.source(parallelized=True)
def machinestate(
    bucket_name: str,
    measurements_to_load: List[str],
    default_backfill_times: Tuple[dt.datetime, dt.datetime],
    last_loaded_times: Dict[str, dt.datetime],
) -> Generator[DltResource]:
    def increment(t: dt.datetime) -> dt.datetime:
        return t + dt.timedelta(microseconds=1)

    # load all measurement data
    last_time_values = last_loaded_times if last_loaded_times is not None else {}
    additional_table_hints = {PARTITION_HINT: [("time", YEAR)]}
    for channel_name in measurements_to_load:
        initial_time_value = (
            increment(last_time_values[channel_name])
            if channel_name in last_time_values
            else default_backfill_times[0]
        )
        end_time_value = default_backfill_times[1]
        time_args = {
            "time": dlt.sources.incremental(
                initial_value=initial_time_value, end_value=end_time_value
            )
        }
        table_name = measurement_channel_table_name(bucket_name, channel_name)
        yield influxdb_get_measurement(
            bucket_name, channel_name, **time_args
        ).with_name(table_name).apply_hints(
            table_name=table_name, additional_table_hints=additional_table_hints
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--channels",
        nargs="+",
        default=[],
        help="A whitespace-separate list of channel names to load to the warehouse. By default all channels from the source will be loaded.",
    )

    return parser.parse_args()


# ------------------------------------------------------------------------------
def main():
    args = parse_args()
    logging.basicConfig(level=logging.INFO)

    LOGGER.info(f"-- {PIPELINE_NAME} --")
    LOGGER.debug(f"Querying channels to load...")
    channels_to_load = (
        args.channels
        if args.channels
        else influxdb_channel_names(
            INFLUXDB_MACHINESTATE_BUCKET,
            dlt.config["sources.base_url"],
            dlt.secrets["sources.auth_token"],
        )
    )
    channels_total_size = len(channels_to_load)
    LOGGER.debug(f"Found {channels_total_size} to load.")

    LOGGER.debug(f"Determining range of previously loaded data...")
    query_last_loaded_times_started_at = dt.datetime.now()
    channel_last_loaded_times = measurement_channel_last_times(
        PIPELINE_NAME, NAMESPACE_NAME, INFLUXDB_MACHINESTATE_BUCKET, channels_to_load
    )
    query_last_loaded_times_finished_at = dt.datetime.now()
    LOGGER.debug(
        f"Queried existing channel times took {humanize.precisedelta(query_last_loaded_times_finished_at - query_last_loaded_times_started_at)}"
    )
    LOGGER.debug(f"Found {len(channel_last_loaded_times)} channels loaded previously.")

    # Begin pipeline
    pipeline = dlt.pipeline(
        destination=pyiceberg(namespace_name=NAMESPACE_NAME),
        pipeline_name=PIPELINE_NAME,
        progress="log",
    )
    backfill_started_at = dt.datetime.now()
    channels_batch_size = dlt.config["sources.channel_batch_size"]
    channels_total_batches = int(channels_total_size / channels_batch_size) + (
        1 if channels_total_size % channels_batch_size > 0 else 0
    )
    index_start, index_end = 0, min(channels_batch_size, channels_total_size)
    batch_number = 1
    while index_start < index_end:
        LOGGER.info(f"Beginning pipeline run {batch_number}/{channels_total_batches}")
        chunk_channels = channels_to_load[index_start:index_end]
        load_info = pipeline.run(
            machinestate(
                INFLUXDB_MACHINESTATE_BUCKET,
                measurements_to_load=chunk_channels,
                default_backfill_times=INFLUXDB_MACHINESTATE_TIMES,
                last_loaded_times=channel_last_loaded_times,
            ),
            write_disposition="append",
        )
        LOGGER.info(load_info)
        LOGGER.info(
            f"Pipeline run for channels ({chunk_channels}) completed in {
            humanize.precisedelta(
                pipeline.last_trace.finished_at - pipeline.last_trace.started_at
            )}"
        )
        # next chunk
        index_start = index_end
        index_end = index_start + min(
            channels_batch_size, channels_total_size - index_start
        )
        batch_number += 1

    backfill_ended_at = dt.datetime.now()
    LOGGER.info(
        f"Backfill from ({backfill_ended_at} -> {backfill_started_at}) completed in {
        humanize.precisedelta(
            backfill_ended_at - backfill_started_at
        )}"
    )


if __name__ == "__main__":
    main()
