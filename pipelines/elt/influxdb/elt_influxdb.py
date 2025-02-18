import argparse
from collections.abc import Generator
import datetime as dt
import logging
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

import dlt
from dlt.extract import DltResource
import humanize
import pandas as pd
import requests

from pipelines_common.constants import SOURCE_NAMESPACE_PREFIX, SOURCE_TABLE_PREFIX
from pipelines_common.destinations.pyiceberg import pyiceberg

LOGGER = logging.getLogger(__name__)


DEVEL = os.environ.get("DEVEL", "true") == "true"
PIPELINE_NAME = f"{"dev_" if DEVEL else ""}elt_influxdb"
NAMESPACE_NAME = f"{SOURCE_NAMESPACE_PREFIX}influxdb"

# Source
INFLUXDB_MACHINESTATE_BUCKET = "machinestate"
INFLUXDB_MACHINESTATE_TIME_START, INFLUXDB_MACHINESTATE_TIME_STOP = (
    dt.datetime(2017, 1, 1),
    dt.datetime(2025, 1, 1),
)

# Destination
COLUMN_CHANNEL_NAME = "channel_name"


def to_utc_str(timestamp: dt.datetime) -> str:
    return timestamp.astimezone(ZoneInfo("UTC")).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def influxdb_measurement_names(
    bucket_name: str,
    base_url: str = dlt.config.value,
    auth_token: str = dlt.secrets.value,
):
    resp = requests.get(
        f"{base_url}/query",
        headers={"Authorization": f"Token {auth_token}"},
        params={"db": bucket_name, "q": f"SHOW MEASUREMENTS"},
    )
    resp_json = resp.json()
    for all_series in resp_json["results"]:
        for series in all_series["series"]:
            yield pd.DataFrame.from_records(series["values"], columns=series["columns"])


@dlt.resource()
def influxdb_get_measurement(
    bucket_name: str,
    channel_name: str,
    time_start: dt.datetime,
    time_stop: dt.datetime,
    base_url: str = dlt.config.value,
    auth_token: str = dlt.secrets.value,
):
    chunk_t0, chunk_t1 = time_start, time_start + dt.timedelta(weeks=52)
    while chunk_t0 < time_stop:
        qt0, qt1 = to_utc_str(chunk_t0), to_utc_str(chunk_t1)
        query = f"SELECT \"value\" FROM /^{channel_name}$/ WHERE time >= '{qt0}' AND time < '{qt1}'"
        resp = requests.get(
            f"{base_url}/query",
            headers={"Authorization": f"Token {auth_token}"},
            params={"db": bucket_name, "q": query},
        )
        resp_json = resp.json()
        for all_series in resp_json["results"]:
            # Do we have any results for this time regime
            if "series" not in all_series:
                break
            for series in all_series["series"]:
                yield pd.DataFrame.from_records(
                    series["values"], columns=series["columns"]
                )
        # next chunk
        chunk_t0 = chunk_t1
        chunk_t1 = min(chunk_t1 + dt.timedelta(weeks=52), time_stop)


# machinestate pipeline
@dlt.source(parallelized=True)
def machinestate(
    bucket_name: str,
    time_start: dt.datetime,
    time_stop: dt.datetime,
    selected_measurements: Optional[List[Dict[str, str]]] = None,
) -> Generator[DltResource]:
    #
    measurements_to_load = (
        influxdb_measurement_names(
            bucket_name=bucket_name,
            base_url=dlt.config["sources.base_url"],
            auth_token=dlt.secrets["sources.auth_token"],
        )
        if selected_measurements is None
        else selected_measurements
    )

    # first, load all measurement data
    for measurement in measurements_to_load:
        channel_name = measurement["name"]
        table_name = f"{SOURCE_TABLE_PREFIX}{bucket_name}_{channel_name}"
        yield influxdb_get_measurement(
            bucket_name, channel_name, time_start, time_stop
        ).with_name(table_name).apply_hints(table_name=table_name)

    # finally load table of measurement names that have been loaded
    yield dlt.resource(
        measurements_to_load,
        name=f"{SOURCE_TABLE_PREFIX}{bucket_name}_measurement",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--channels", nargs="+", default=None)

    return parser.parse_args()


# ------------------------------------------------------------------------------
# Main
args = parse_args()
logging.basicConfig(level=logging.INFO)


pipeline = dlt.pipeline(
    destination=pyiceberg(namespace_name=NAMESPACE_NAME),
    pipeline_name=PIPELINE_NAME,
    progress="log",
)

load_info = pipeline.run(
    machinestate(
        INFLUXDB_MACHINESTATE_BUCKET,
        INFLUXDB_MACHINESTATE_TIME_START,
        INFLUXDB_MACHINESTATE_TIME_STOP,
        selected_measurements=(
            [{"name": name} for name in args.channels]
            if args.channels is not None
            else None
        ),
    ),
    write_disposition="replace",
)

LOGGER.info(load_info)
LOGGER.info(
    f"Pipeline run completed in {
    humanize.precisedelta(
        pipeline.last_trace.finished_at - pipeline.last_trace.started_at
    )}"
)
