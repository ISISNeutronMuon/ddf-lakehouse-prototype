"""Utility functions for a cli script"""

import argparse
from enum import StrEnum
import logging
from typing import Any, Dict, Sequence

# String argument name with its associated configuration
type CliArgConfig = Dict[str, Any]


class CommonCliArg(StrEnum):
    LOG_LEVEL = "--log-level"
    ON_PIPELINE_STEP_FAILURE = "--on-pipeline-step-failure"

    # SKIP_EXTRACT_AND_LOAD = "--skip-extract-and-load"
    # SKIP_TRANSFORM = "--skip-transform"


# This defines the list of cli args common to all pipelines
_COMMON_CLI_ARG_CONFIG: Dict[str, CliArgConfig] = {
    CommonCliArg.LOG_LEVEL: dict(
        default=logging.getLevelName(logging.INFO),
        choices=logging.getLevelNamesMapping().keys(),
    ),
    CommonCliArg.ON_PIPELINE_STEP_FAILURE: dict(
        type=str,
        default="raise",
        choices=["raise", "log_and_continue"],
        help="What should be done with pipeline step failure exceptions",
    ),
}


def create_common_argparser() -> argparse.ArgumentParser:
    """Create an argparser with options that should be common to all pipeline scripts"""
    return create_argparser(_COMMON_CLI_ARG_CONFIG)


def create_argparser(
    cli_args: Dict[str, CliArgConfig],
) -> argparse.ArgumentParser:
    """Creates an ArgumentParser with the chosen common options plus any extras provided"""
    return append_args(argparse.ArgumentParser(), cli_args)


def append_args(
    parser: argparse.ArgumentParser, cli_args: Dict[str, CliArgConfig]
) -> argparse.ArgumentParser:
    """Given an ArgumentParser append further items to the list of accepted arguments"""
    for arg_name, arg_config in cli_args.items():
        parser.add_argument(arg_name, **arg_config)

    return parser
