"""Utility functions for a cli script"""

import argparse
import logging
import typing
from typing import Any

import dlt
from dlt.common.destination.reference import TDestinationReferenceArg
from dlt.common.typing import TLoaderFileFormat
from dlt.common.schema.typing import TWriteDisposition
from dlt.pipeline.progress import TCollectorArg, _NULL_COLLECTOR as NULL_COLLECTOR
import humanize

from . import logging as logging_utils

LOGGER = logging.getLogger(__name__)


def create_standard_argparser(
    default_destination: TDestinationReferenceArg,
    default_write_disposition: TWriteDisposition,
    default_loader_file_format: TLoaderFileFormat,
    default_progress: TCollectorArg,
) -> argparse.ArgumentParser:
    """Creates an ArgumentParser with standard options common to most pipelines"""
    parser = argparse.ArgumentParser()
    parser.add_argument("--log-level", choices=logging.getLevelNamesMapping().keys())
    parser.add_argument(
        "--on-pipeline-step-failure",
        type=str,
        default="raise",
        choices=["raise", "log_and_continue"],
        help="What should be done with pipeline step failure exceptions",
    )
    parser.add_argument(
        "--destination",
        default=default_destination,
        help="Destination for the loaded data.",
    )
    parser.add_argument(
        "--write-disposition",
        default=default_write_disposition,
        choices=typing.get_args(TWriteDisposition),
        help="The write disposition used with dlt.",
    )
    parser.add_argument(
        "--loader-file-format",
        default=default_loader_file_format,
        help="The dlt loader file format",
    )
    parser.add_argument(
        "--progress",
        default=default_progress,
        help="The dlt progress option",
    )

    return parser


def cli_main(
    pipeline_name: str,
    data: Any,
    dataset_name: str | None = None,
    *,
    default_write_disposition: TWriteDisposition = "append",
    default_destination: TDestinationReferenceArg = "filesystem",
    default_loader_file_format: TLoaderFileFormat = "parquet",
    default_progress: TCollectorArg = NULL_COLLECTOR,
):
    """Run a standard extract and load pipeline"""
    args = create_standard_argparser(
        default_destination,
        default_write_disposition,
        default_loader_file_format,
        default_progress,
    ).parse_args()
    # TODO: Make the log filtering more configurable
    logging_utils.configure_logging(
        args.log_level, keep_records_from=["dlt", "pipelines_common", "__main__"]
    )

    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        dataset_name=dataset_name,
        destination=args.destination,
        progress=args.progress,
    )
    LOGGER.info(f"-- Running pipeline={pipeline.pipeline_name} --")
    pipeline.run(
        data,
        loader_file_format=args.loader_file_format,
        write_disposition=args.write_disposition,
    )
    LOGGER.debug(pipeline.last_trace.last_extract_info)
    LOGGER.debug(pipeline.last_trace.last_load_info)
    LOGGER.info(
        f"Pipeline {pipeline.pipeline_name} completed in {
        humanize.precisedelta(
            pipeline.last_trace.finished_at - pipeline.last_trace.started_at
        )}"
    )
