"""Utility functions for a cli script"""

import argparse
import logging


def create_standard_argparser() -> argparse.ArgumentParser:
    """Creates an ArgumentParser with standard options common to most pipelines"""
    parser = argparse.ArgumentParser()
    parser.add_argument("--log-level", choices=logging.getLevelNamesMapping().keys())
    parser.add_argument(
        "--skip-extract-and-load",
        action="store_true",
        help="Skip the extract and load step.",
    )
    parser.add_argument(
        "--skip-transfrom",
        action="store_true",
        help="Skip the extract and load step.",
    )

    return parser
