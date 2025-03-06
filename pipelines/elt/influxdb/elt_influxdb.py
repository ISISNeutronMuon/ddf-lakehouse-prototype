import argparse
from collections.abc import Generator
import logging
import pandas as pd
from typing import List, Optional, Tuple
from zoneinfo import ZoneInfo

import dlt
from dlt.extract import DltResource
from dlt.pipeline.exceptions import PipelineStepFailed

import humanize
import pandas as pd
import pendulum
import requests

from pipelines_common.constants import (
    MICROSECONDS_STR,
    MICROSECONDS_STR_INFLUX,
)

from pipelines_common.pipeline import pipeline_name

# Runtime
LOGGER = logging.getLogger(__name__)
PIPELINE_BASENAME = "influxdb"

# Source
INFLUXDB_MACHINESTATE_BUCKET = "machinestate"
INFLUXDB_MACHINESTATE_BACKFILL_START = pendulum.DateTime(
    2018, 1, 1, tzinfo=pendulum.UTC
)

# Staging destination
COLUMN_CHANNEL_NAME = "channel"
LOADER_FILE_FORMAT = "parquet"


def _to_utc_str(timestamp: pendulum.DateTime) -> str:
    return timestamp.astimezone(ZoneInfo("UTC")).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


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
        return list(
            filter(lambda x: not x.startswith("aws"), flatten(series["values"]))
        )
    except (IndexError, KeyError):
        raise ValueError(
            f"Error querying Influx for available channel names. Unexpected result structure:\n '{resp_json}'"
        )


@dlt.resource()
def influxdb_get_measurement(
    bucket_name: str,
    channel_name: str,
    time: dlt.sources.incremental = dlt.sources.incremental("time"),
    base_url: str = dlt.config.value,
    auth_token: str = dlt.secrets.value,
):
    """Load data for a given channel.

    The start time of the load is determined through the dlt.incremental mechanism tracking the
    value of the last load. If no end time is specified the current date/time is used.
    """

    def next_chunk_start(ts: pendulum.DateTime) -> pendulum.DateTime:
        # A bug in Pendulum drops the timezone on addition with days:
        # https://github.com/python-pendulum/pendulum/issues/669
        return (ts.add(weeks=52)).astimezone(pendulum.UTC)

    time_start = time.start_value
    time_end = (
        time.end_value if time.end_value is not None else pendulum.now(pendulum.UTC)
    )
    LOGGER.debug(
        f"Pulling measurement '{channel_name}' for time range [{time_start}, {time_end})"
    )

    chunk_start, chunk_end = time_start, min(next_chunk_start(time_start), time_end)
    while chunk_start < chunk_end:
        query_t0, query_t1 = _to_utc_str(chunk_start), _to_utc_str(chunk_end)
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
                    df["time"],
                    origin="unix",
                    unit=MICROSECONDS_STR,
                    utc=True,
                )
                df["value"] = df["value"].astype("float64")
                df[COLUMN_CHANNEL_NAME] = channel_name
                # Sort by time so the incremental loading mechanism gets the correct "last value"
                df.sort_values(by=["time"], inplace=True)
                yield df
        # next chunk
        chunk_start = chunk_end
        chunk_end = min(next_chunk_start(chunk_end), time_end)


# "name" defines the name of the destination schema, either folder inside
# dataset on filesystem or schema in destination DB
@dlt.source(parallelized=True, name=INFLUXDB_MACHINESTATE_BUCKET)
def machinestate(
    bucket_name: str,
    measurements_to_load: List[str],
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
        yield influxdb_get_measurement(
            bucket_name, channel_name, **time_args
        ).with_name(table_name).apply_hints(table_name=table_name)


def extract_and_load_machinestate(
    pipeline: dlt.Pipeline, channels: List[str], on_pipline_step_failed: str
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
                INFLUXDB_MACHINESTATE_BUCKET,
                measurements_to_load=channels,
                backfill_range=(INFLUXDB_MACHINESTATE_BACKFILL_START, None),
            ),
            loader_file_format=LOADER_FILE_FORMAT,
            write_disposition="append",
        )
        LOGGER.debug(load_info)
    except PipelineStepFailed as exc:
        if on_pipline_step_failed == "raise":
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--channels",
        nargs="+",
        default=[],
        help="A whitespace-separate list of channel names to load to the warehouse. By default all channels from the source will be loaded.",
    )
    parser.add_argument(
        "--on-pipeline-step-failure",
        type=str,
        default="raise",
        choices=["raise", "log_and_continue"],
        help="What should be done with pipeline step failure exceptions",
    )

    return parser.parse_args()


# ------------------------------------------------------------------------------
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
    LOGGER.info(f"-- Pipeline={pipeline_name_fq} --")
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

    # Begin pipeline
    pipeline = dlt.pipeline(
        destination="filesystem",
        pipeline_name=pipeline_name_fq,
        dataset_name=pipeline_name_fq,
        progress="log",
    )

    elt_started_at = pendulum.now()
    channels_batch_size = dlt.config["sources.channel_batch_size"]
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
            pipeline, chunk_channels, args.on_pipeline_step_failure
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


if __name__ == "__main__":
    main()
