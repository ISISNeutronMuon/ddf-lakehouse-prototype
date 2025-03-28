from collections.abc import Sequence
from pathlib import Path
from typing import Any


def required_filesystem_tables() -> Sequence[str]:
    """Return a list of the staged filesystem tables required for this model"""
    return (
        "entries",
        "logbook_entries",
        "logbook_chapter",
        "logbooks",
        "more_entry_columns",
        "additional_columns",
    )


def required_catalog_tables() -> Sequence[str]:
    """Return a list of tables within a catalog required by this model"""
    return ("isis.facility.running_schedule",)


def compute(engine: Any):
    with open(Path(__file__).parent / "compute.sql") as fp:
        engine.sql(fp.read())
