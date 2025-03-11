import argparse
from collections.abc import Generator
import itertools
import logging
import pandas as pd
from pathlib import Path
from typing import Any, List, Optional, Sequence, Tuple
import subprocess as subp
import sys
from zoneinfo import ZoneInfo

import dlt
from dlt.common.configuration.resolve import inject_section
from dlt.common.configuration.specs import ConfigSectionContext
from dlt.extract import DltResource
from dlt.pipeline.exceptions import PipelineStepFailed

import humanize
import pandas as pd
import pendulum
import requests

from pipelines_common.constants import (
    MICROSECONDS_STR,
    MICROSECONDS_STR_INFLUX,
    MICROSECONDS_PER_SEC,
)

from pipelines_common.pipeline import pipeline_name

# Runtime
LOGGER = logging.getLogger(__name__)
PIPELINE_BASENAME = "influxdb"
POST_SUBPROCESS_SCRIPT = Path(__file__).parent / "restart-docker.sh"

# Source
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

    def channel_names(self) -> List[str]:
        def flatten(nested):
            return itertools.chain.from_iterable(nested)

        return list(flatten(self._query("SHOW MEASUREMENTS")["values"]))

    def last_time(
        self,
        channel_name: str,
    ) -> pendulum.DateTime:
        query = f'SELECT LAST("value") FROM "{channel_name}"'
        timestamp_us = self._query(query)["values"][0][0]
        return pendulum.from_timestamp(
            timestamp_us / MICROSECONDS_PER_SEC,
            tz="UTC",
        )

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
                "db": self._bucket_name,
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
def influxdb_get_measurement(
    influx: InfluxQuery,
    channel_name: str,
    time: dlt.sources.incremental = dlt.sources.incremental("time"),
    base_url: str = dlt.config.value,
    auth_token: str = dlt.secrets.value,
):
    """Load data for a given channel."""

    def next_chunk_start(ts: pendulum.DateTime) -> pendulum.DateTime:
        # A bug in Pendulum drops the timezone on addition with days:
        # https://github.com/python-pendulum/pendulum/issues/669
        return (ts.add(weeks=52)).astimezone(pendulum.UTC)

    def next_microsecond(ts: pendulum.DateTime) -> pendulum.DateTime:
        return (ts.add(microseconds=1)).astimezone(pendulum.UTC)

    # The start time of the load is determined through the dlt.incremental mechanism tracking the
    # value of the last load. If no end time is specified the last time within Influx
    # is queried and a microsecond is added to ensure we capture all values

    time_start = time.start_value
    time_end = (
        time.end_value
        if time.end_value is not None
        else next_microsecond(influx.last_time(channel_name))
    )
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


# "name" defines the name of the destination schema, either folder inside
# dataset on filesystem or schema in destination DB
@dlt.source(parallelized=True, name="machinestate")
def machinestate(
    influx: InfluxQuery,
    measurements_to_load: Sequence[str],
    backfill_range: Tuple[pendulum.DateTime, Optional[pendulum.DateTime]],
) -> Generator[DltResource]:

    time_end_value = backfill_range[1] if backfill_range[1] is not None else None
    for channel_name in measurements_to_load:
        initial_time_value = backfill_range[0]
        time_args = {
            "time": dlt.sources.incremental(
                initial_value=initial_time_value, end_value=time_end_value
            )
        }
        table_name = channel_name
        yield influxdb_get_measurement(influx, channel_name, **time_args).with_name(
            table_name
        ).apply_hints(table_name=table_name)


def extract_and_load_machinestate(
    pipeline: dlt.Pipeline,
    influx: InfluxQuery,
    channels: Sequence[str],
    on_pipeline_step_failed: str,
):
    """Extract and load the Influxdb machinestate channels to the destination

    :param pipeline: A dlt.pipeline object configured with the appropriate destination
    :param channels: A list of channels to load.
    :param on_pipeline_step_failed: What action to take on a pipeline step failure: options=(raise, log_and_continue)
    """
    load_info = None
    try:
        load_info = pipeline.run(
            machinestate(
                influx,
                measurements_to_load=channels,
                backfill_range=(INFLUXDB_MACHINESTATE_BACKFILL_START, None),
            ),
            loader_file_format=LOADER_FILE_FORMAT,
            write_disposition="append",
        )
        LOGGER.debug(load_info)
    except PipelineStepFailed as exc:
        if on_pipeline_step_failed == "raise":
            raise
        else:
            LOGGER.info(f"Pipeline step failed with error: {str(exc)}. Skipping")
            # If any packages failed to load we don't want to load them again.
            pipeline.drop_pending_packages(with_partial_loads=True)
    LOGGER.info(
        f"Extract & load run for channels ({channels}) completed in {
            humanize.precisedelta(
                pipeline.last_trace.finished_at - pipeline.last_trace.started_at
            )}"
    )

    return load_info


def create_pipeline(pipeline_name_fq: str):
    return dlt.pipeline(
        destination="filesystem",
        pipeline_name=pipeline_name_fq,
        dataset_name=pipeline_name_fq,
        progress="log",
    )


def run_pipeline(
    pipeline: dlt.Pipeline,
    influx: InfluxQuery,
    channels_to_load: Sequence[str],
    on_pipeline_step_failure: str,
):
    elt_started_at = pendulum.now()
    channels_batch_size = dlt.config["sources.channel_batch_size"]
    channels_total_size = len(channels_to_load)
    channels_total_batches = int(channels_total_size / channels_batch_size) + (
        1 if channels_total_size % channels_batch_size > 0 else 0
    )
    index_start, index_end = 0, min(channels_batch_size, channels_total_size)
    batch_number = 1
    while index_start < index_end:
        LOGGER.info(
            f"Beginning extract & load run {batch_number}/{channels_total_batches}"
        )
        chunk_channels = channels_to_load[index_start:index_end]

        load_info = extract_and_load_machinestate(
            pipeline, influx, chunk_channels, on_pipeline_step_failure
        )
        if load_info is not None:
            if LOGGER.level == logging.DEBUG:
                for load_id in load_info.loads_ids:
                    LOGGER.debug(pipeline.get_load_package_info(load_id))

        # next chunk
        index_start = index_end
        index_end = index_start + min(
            channels_batch_size, channels_total_size - index_start
        )
        batch_number += 1

    elt_ended_at = pendulum.now()
    LOGGER.info(
        f"ELT completed in {
        humanize.precisedelta(
            elt_ended_at - elt_started_at
        )}"
    )

    return pipeline


# ------------------------------------------------------------------------------
def get_channels_to_load(
    cli_args: List[str],
    influx: InfluxQuery,
    pipeline: dlt.Pipeline,
    skip_existing: bool,
) -> Sequence[str]:
    """Determine which channels should be loaded, given the arguments

    :param cli_args: Arguments from the command line. These take precendence
    :param influx: InfluxQuery object
    :param pipeline: A dlt.Pipeline object that can query the destination
    :param skip_existing: If true, filter out channels that already exist in the destination
    """
    if cli_args:
        channels_to_load = cli_args
    else:
        channels_to_load = influx.channel_names()

    if skip_existing:
        with inject_section(ConfigSectionContext(pipeline.pipeline_name)):
            existing_tables = pipeline.dataset().schema.data_table_names(
                seen_data_only=True
            )
            existing_channels = set(
                map(
                    lambda x: x.replace("_", "::", 1).replace("_", ":"), existing_tables
                )
            )
            channels_to_load = list(
                set(map(lambda x: x.lower(), channels_to_load)) - set(existing_channels)
            )

    return channels_to_load


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--channels",
        nargs="+",
        default=[],
        help="A whitespace-separate list of channel names to load to the warehouse. By default all channels from the source will be loaded.",
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="If provided then don't query Influx for new data for channels that exist in the destination.",
    )
    parser.add_argument(
        "--num-per-process",
        type=int,
        default=1000,
        help="The number of channels to handle in a single process."
        "If more channels than this are found then the processing is split into multiple processes to keep memory usage under control.",
    )
    parser.add_argument(
        "--on-pipeline-step-failure",
        type=str,
        default="raise",
        choices=["raise", "log_and_continue"],
        help="What should be done with pipeline step failure exceptions",
    )

    return parser.parse_args()


def configure_logging(root_level: int):
    class FilterUnwantedRecords:
        def filter(self, record):
            return record.name == "__main__"

    logging.basicConfig(level=root_level)
    for handler in logging.getLogger().handlers:
        handler.addFilter(FilterUnwantedRecords())


def main():
    args = parse_args()
    configure_logging(logging.DEBUG)

    pipeline_name_fq = pipeline_name(PIPELINE_BASENAME)
    pipeline = create_pipeline(pipeline_name_fq)
    LOGGER.info(f"-- Pipeline={pipeline_name_fq} created --")

    influx = InfluxQuery(
        dlt.config["sources.bucket_name"],
        dlt.config["sources.base_url"],
        dlt.secrets["sources.auth_token"],
    )
    channels_to_load = get_channels_to_load(
        args.channels, influx, pipeline, args.skip_existing
    )
    channels_total_size = len(channels_to_load)
    if channels_total_size == 0:
        LOGGER.info("No channels have been found to load. Exiting.")
        return

    # Keep memory under control for a large number of channels by
    # running in subprocesses
    LOGGER.debug(f"Found {channels_total_size} to load.")
    num_channels_per_process = args.num_per_process
    if channels_total_size <= num_channels_per_process:
        # Run a single process
        LOGGER.debug(f"Using single process to load all requested channels.")
        LOGGER.debug(f"Loading channels {channels_to_load}")
        run_pipeline(pipeline, influx, channels_to_load, args.on_pipeline_step_failure)
    else:
        # Batch up in subprocesses
        LOGGER.debug(
            f"Using a subprocesses to load {num_channels_per_process} channels per process."
        )
        channels_batched = list(
            itertools.batched(channels_to_load, num_channels_per_process)
        )
        for subp_index, channels_per_process in enumerate(channels_batched):
            LOGGER.info(f"Starting subprocess {subp_index+1}/{len(channels_batched)}")
            cmd = [
                sys.executable,
                __file__,
                "--on-pipeline-step-failure",
                args.on_pipeline_step_failure,
            ]
            if args.skip_existing:
                cmd.append("--skip-existing")
            cmd.append("--channels")
            cmd.extend(channels_per_process)
            LOGGER.debug(f"Executing '{cmd}'")
            subp.run(cmd, check=True)
            LOGGER.info(f"Completed subprocess {subp_index+1}/{len(channels_batched)}")

            if POST_SUBPROCESS_SCRIPT.exists():
                LOGGER.debug("Running post-processing script.")
                subp.run([POST_SUBPROCESS_SCRIPT], check=True)


if __name__ == "__main__":
    main()
