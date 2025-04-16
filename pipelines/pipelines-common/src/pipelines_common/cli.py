"""Utility functions for a cli script"""

import argparse
import logging
from typing import Any

import dlt
from dlt.common.destination.reference import TDestinationReferenceArg
from dlt.common.typing import TLoaderFileFormat
from dlt.common.schema.typing import TWriteDispositionConfig
from dlt.pipeline.progress import TCollectorArg, _NULL_COLLECTOR as NULL_COLLECTOR
import humanize

from . import logging as logging_utils

LOGGER = logging.getLogger(__name__)


def create_standard_argparser() -> argparse.ArgumentParser:
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

    return parser


def cli_main(
    pipeline_name: str,
    data: Any,
    write_disposition: TWriteDispositionConfig,
    destination: TDestinationReferenceArg = "filesystem",
    loader_file_format: TLoaderFileFormat = "parquet",
    dataset_name: str | None = None,
    progress: TCollectorArg = NULL_COLLECTOR,
):
    """Run a standard extract and load pipeline"""
    args = create_standard_argparser().parse_args()
    logging_utils.configure_logging(args.log_level, keep_records_from=["__main__"])

    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        dataset_name=(dataset_name if dataset_name is not None else None),
        destination=destination,
        progress=progress,
    )
    LOGGER.info(f"-- Running pipeline={pipeline.pipeline_name} --")
    pipeline.run(
        data, loader_file_format=loader_file_format, write_disposition=write_disposition
    )
    LOGGER.debug(pipeline.last_trace.last_extract_info)
    LOGGER.debug(pipeline.last_trace.last_load_info)
    LOGGER.info(
        f"Pipeline {pipeline.pipeline_name} completed in {
        humanize.precisedelta(
            pipeline.last_trace.finished_at - pipeline.last_trace.started_at
        )}"
    )
