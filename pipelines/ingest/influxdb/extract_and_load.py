#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "dlt>=1.10.0,<1.11.0",
#     "humanize>=4.12.3,<4.13.0",
#     "pandas>=2.2.3,<2.3.0",
#     "pipelines-common",
#     "psutil>=7.0.0,<7.1.0",
#     "requests>=2.32.4,<2.33.0",
# ]
#
# [tool.uv.sources]
# pipelines-common = { path = "../../pipelines-common" }
# ///
import itertools
import logging
import multiprocessing as mp
from typing import Any, List, Optional, Sequence, cast
from zoneinfo import ZoneInfo

import dlt

import humanize
import pandas as pd
import pendulum
import requests

import pipelines_common.cli as cli_utils
import pipelines_common.logging as logging_utils
import pipelines_common.pipeline as pipeline_utils
from pipelines_common.dlt_destinations.pyiceberg.pyiceberg import PyIcebergClient
from pipelines_common.dlt_destinations.pyiceberg.pyiceberg_adapter import (
    pyiceberg_adapter,
    pyiceberg_partition,
    PartitionTransformation,
)


from pipelines_common.constants import (
    MICROSECONDS_STR,
    MICROSECONDS_STR_INFLUX,
    MICROSECONDS_PER_SEC,
)

# Runtime
LOGGER = logging.getLogger(__name__)

# Source
SCHEMA_NAME_DELIMITER = "::"
INFLUXDB_MACHINESTATE_BACKFILL_START = pendulum.DateTime(
    2018, 1, 1, tzinfo=pendulum.UTC
)

# Staging destination
COLUMN_CHANNEL_NAME = "channel"
LOADER_FILE_FORMAT = "parquet"


def _to_utc_str(timestamp: pendulum.DateTime) -> str:
    return timestamp.astimezone(ZoneInfo("UTC")).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


class InfluxQuery:
    """Perform an influx query on a named bucket at a url and with the given auth token"""

    def __init__(self, bucket_name: str, base_url: str, auth_token: str):
        self._bucket_name = bucket_name
        self._base_url = base_url
        self._auth_token = auth_token

    @property
    def bucket_name(self) -> str:
        return self._bucket_name

    def channel_names(self) -> List[str]:
        def flatten(nested):
            return itertools.chain.from_iterable(nested)

        return list(flatten(self._query("SHOW MEASUREMENTS")["values"]))

    def last_time(
        self,
        channel_name: str,
    ) -> pendulum.DateTime | None:
        query = f'SELECT LAST("value") FROM "{channel_name}"'
        try:
            timestamp_us = self._query(query)["values"][0][0]
            return pendulum.from_timestamp(
                timestamp_us / MICROSECONDS_PER_SEC,
                tz="UTC",
            )
        except ValueError:
            return None

    def select_channel(
        self,
        channel_name: str,
        time_start: pendulum.DateTime,
        time_end: pendulum.DateTime,
    ) -> Optional[pd.DataFrame]:
        query_t0, query_t1 = _to_utc_str(time_start), _to_utc_str(time_end)
        query = f"SELECT \"value\" FROM \"{channel_name}\" WHERE time >= '{query_t0}' AND time < '{query_t1}'"
        try:
            series = self._query(query)
            df = pd.DataFrame.from_records(series["values"], columns=series["columns"])
        except ValueError:
            return None

        df["time"] = pd.to_datetime(
            df["time"],
            origin="unix",
            unit=MICROSECONDS_STR,
            utc=True,
        )
        df["value"] = df["value"].astype("float64")
        df[COLUMN_CHANNEL_NAME] = channel_name
        # Sort by time so the incremental loading mechanism gets the correct "last value"
        df.sort_values(by=["time"], inplace=True)
        return df

    # Private
    def _query(self, query: str) -> Any:
        """Perform a query against the REST api of the influx database. Returns the series field if present"""
        LOGGER.debug(query)
        resp = requests.get(
            f"{self._base_url}/query",
            headers={"Authorization": f"Token {self._auth_token}"},
            params={
                "db": self.bucket_name,
                "epoch": MICROSECONDS_STR_INFLUX,
                "q": query,
            },
        )
        resp_json = resp.json()
        try:
            return resp_json["results"][0]["series"][0]
        except (IndexError, KeyError):
            raise ValueError(
                f"Error querying Influx. Unexpected result structure:\n '{resp_json}'"
            )


@dlt.resource()
def influxdb_measurement(influx: InfluxQuery, channel_name: str, full_load: bool):
    """Load data for a given channel."""

    def next_chunk_start(ts: pendulum.DateTime) -> pendulum.DateTime:
        # A bug in Pendulum drops the timezone on addition with days:
        # https://github.com/python-pendulum/pendulum/issues/669
        return (ts.add(weeks=52)).astimezone(pendulum.UTC)

    def next_microsecond(ts: pendulum.DateTime) -> pendulum.DateTime:
        return (ts.add(microseconds=1)).astimezone(pendulum.UTC)

    # If a full load is not requested the start time of the load is determined by checking the
    # latest loaded value in the destination
    time_start = INFLUXDB_MACHINESTATE_BACKFILL_START
    if not full_load:
        current_pipeline = dlt.current.pipeline()
        catalog = cast(
            PyIcebergClient, current_pipeline.destination_client()
        ).iceberg_catalog
        table_id = (current_pipeline.dataset_name, influx.bucket_name)
        if catalog.table_exists(table_id):
            table = catalog.load_table(table_id)
            df = (
                table.scan(
                    row_filter=f"channel == '{channel_name}'",
                    selected_fields=("channel", "time"),
                )
                .to_arrow()
                .sort_by([("time", "descending")])
            )
            try:
                time_start = next_microsecond(
                    pendulum.instance(
                        df.slice(offset=0, length=1)["time"].to_pylist()[0]
                    )
                )
            except IndexError:
                # No data in table
                pass

    # Consume everything influx has by setting the end time just past the last time value.
    last_time_influx = influx.last_time(channel_name)
    if last_time_influx:
        time_end = next_microsecond(last_time_influx)
    else:
        LOGGER.debug(
            f"No last time value found for '{channel_name}'. Assuming no data available."
        )
        return None
    LOGGER.debug(
        f"Pulling measurement '{channel_name}' for time range [{time_start}, {time_end})"
    )

    chunk_start, chunk_end = time_start, min(next_chunk_start(time_start), time_end)
    while chunk_start < chunk_end:
        df = influx.select_channel(channel_name, chunk_start, chunk_end)
        yield df
        # next chunk
        chunk_start = chunk_end
        chunk_end = min(next_chunk_start(chunk_end), time_end)


def destination_partition_config(
    bucket_name: str,
) -> List[str | PartitionTransformation]:
    """Create a partition config for the destination based on the Influx bucket"""
    if bucket_name == "machinestate":
        return ["channel", pyiceberg_partition.year("time")]
    else:
        raise ValueError(
            f"Partition configuration: Unknown bucket_name: '{bucket_name}'."
        )


def run_pipeline(
    args: cli_utils.argparse.Namespace,
    influx: InfluxQuery,
    channels_to_load: Sequence[str],
):
    logging_utils.configure_logging(args.log_level)

    pipeline = pipeline_utils.create_pipeline(
        name="influxdb",
        destination=args.destination,
        progress=args.progress,
    )
    LOGGER.info(f"Pipeline:{pipeline.pipeline_name}")
    LOGGER.info(f"Loading {len(channels_to_load)} channels")

    elt_started_at = pendulum.now()
    for channel in channels_to_load:
        LOGGER.info(f"Loading channel '{channel}'")
        pipeline.drop_pending_packages()
        resource = pyiceberg_adapter(
            influxdb_measurement(
                influx, channel, full_load=(args.write_disposition == "replace")
            ).with_name(influx.bucket_name),
            partition=destination_partition_config(influx.bucket_name),
        )
        load_info = pipeline.run(
            resource,
            loader_file_format=LOADER_FILE_FORMAT,
            write_disposition=args.write_disposition,
        )
        LOGGER.debug(load_info)

    elt_ended_at = pendulum.now()
    LOGGER.info(
        f"ELT for {len(channels_to_load)} channels completed in {
            humanize.precisedelta(elt_ended_at - elt_started_at)
        }"
    )

    return pipeline


# ------------------------------------------------------------------------------


def get_channels_to_load(cli_args: List[str], influx: InfluxQuery) -> List[str]:
    """Determine which channels should be loaded, given the arguments from the cli.

    :param cli_args: Arguments from the command line. These take precendence.
    :param influx: InfluxQuery object to query all of the available channels
    :return: A list of channel names, sorted alphabetically.
    """
    channels = cli_args if cli_args else influx.channel_names()
    return sorted(channels)


def parse_args() -> cli_utils.argparse.Namespace:
    parser = cli_utils.create_standard_argparser(
        default_destination="pipelines_common.dlt_destinations.pyiceberg",
        default_write_disposition="append",
        default_progress="log",
        default_loader_file_format="parquet",
    )
    parser.add_argument(
        "--channels",
        nargs="+",
        default=[],
        help="A whitespace-separate list of channel names to load to the warehouse. By default all channels from the source will be loaded.",
    )

    return parser.parse_args()


def main():
    cli_args = parse_args()
    logging_utils.configure_logging(cli_args.log_level, keep_records_from=["__main__"])
    influx = InfluxQuery(
        dlt.config["influxdb.bucket_name"],
        dlt.config["influxdb.base_url"],
        dlt.secrets["influxdb.auth_token"],
    )
    channels_to_load = get_channels_to_load(cli_args.channels, influx)
    if not channels_to_load:
        LOGGER.info("No channels have been found to load. Exiting.")
        return

    # If run for a large number of channels then the memory of the main process can creep up.
    # Using subprocesses (but not in parallel) helps keep the memory under control.
    mp.set_start_method("spawn")
    for channels_batch in itertools.batched(
        channels_to_load, dlt.config["influxdb.channels_per_subprocess"]
    ):
        process = mp.Process(
            target=run_pipeline, args=(cli_args, influx, channels_batch)
        )
        process.start()
        process.join()


if __name__ == "__main__":
    main()
