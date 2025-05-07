from typing import Any, Dict, Sequence, Union, Final

from dlt.destinations.utils import get_resource_for_adapter
from dlt.extract import DltResource
from dlt.extract.items import TTableHintTemplate

import pyiceberg.transforms as pytr

PARTITION_HINT: Final[str] = "x-pyiceberg-partition"

# This module is a copy of the athena_adapter from
# dlt/destinations/impl/athena/athena_adatper.py adapted to the pyiceberg
# destination.


class PartitionTransformation:
    transform: str
    """The transform as a string representation understood by pyicberg.transforms.parse_trasnform., e.g. `bucket[16]`"""
    column_name: str
    """Column name to apply the transformation to"""

    def __init__(self, transform: str, column_name: str) -> None:
        self.transform = transform
        self.column_name = column_name


class pyiceberg_partition:
    """Helper class to generate iceberg partition transformations"""

    @staticmethod
    def identity(column_name: str) -> PartitionTransformation:
        """Partition by column without an transformation"""
        return PartitionTransformation(pytr.IDENTITY, column_name)

    @staticmethod
    def year(column_name: str) -> PartitionTransformation:
        """Partition by year part of a date or timestamp column."""
        return PartitionTransformation(pytr.YEAR, column_name)

    @staticmethod
    def month(column_name: str) -> PartitionTransformation:
        """Partition by month part of a date or timestamp column."""
        return PartitionTransformation(pytr.MONTH, column_name)

    @staticmethod
    def day(column_name: str) -> PartitionTransformation:
        """Partition by day part of a date or timestamp column."""
        return PartitionTransformation(pytr.DAY, column_name)

    @staticmethod
    def hour(column_name: str) -> PartitionTransformation:
        """Partition by hour part of a date or timestamp column."""
        return PartitionTransformation(pytr.HOUR, column_name)

    # NOTE: The following transformations are not currently supported by writing through
    # pyarrow so they are disabled.

    # @staticmethod
    # def bucket(n: int, column_name: str) -> PartitionTransformation:
    #     """Partition by hashed value to n buckets."""
    #     return PartitionTransformation(f"{pytr.BUCKET}[{n}]", column_name)

    # @staticmethod
    # def truncate(length: int, column_name: str) -> PartitionTransformation:
    #     """Partition by value truncated to length."""
    #     return PartitionTransformation(f"{pytr.TRUNCATE}[{length}]", column_name)


def pyiceberg_adapter(
    data: Any,
    partition: Union[
        str, PartitionTransformation, Sequence[Union[str, PartitionTransformation]]
    ] = None,
) -> DltResource:
    """
    Prepares data for loading into pyiceberg

    Args:
        data: The data to be transformed.
            This can be raw data or an instance of DltResource.
            If raw data is provided, the function will wrap it into a `DltResource` object.
        partition: Column name(s) or instances of `PartitionTransformation` to partition the table by.

    Returns:
        A `DltResource` object that is ready to be loaded into a pyiceberg catalog.

    Raises:
        ValueError: If any hint is invalid or none are specified.

    Examples:
        >>> data = [{"name": "Marcel", "department": "Engineering", "date_hired": "2024-01-30"}]
        >>> pyiceberg_adapter(data, partition=["department", pyiceberg_partition.year("date_hired"), pyiceberg_partition.bucket(8, "name")])
        [DltResource with hints applied]
    """
    resource = get_resource_for_adapter(data)
    additional_table_hints: Dict[str, TTableHintTemplate[Any]] = {}

    if partition:
        if isinstance(partition, str) or not isinstance(partition, Sequence):
            partition = [partition]

        # Partition hint is `{column_name: template}`, e.g. `{"department": "{column_name}", "date_hired": "year({column_name})"}`
        # Use one dict for all hints instead of storing on column so order is preserved
        partition_hint: Dict[str, str] = {}

        for item in partition:
            if isinstance(item, PartitionTransformation):
                # Client will generate the final SQL string with escaped column name injected
                partition_hint[item.column_name] = item.transform
            else:
                # Item is the column name
                partition_hint[item] = pyiceberg_partition.identity(item).transform

        additional_table_hints[PARTITION_HINT] = partition_hint

    if additional_table_hints:
        resource.apply_hints(additional_table_hints=additional_table_hints)
    else:
        raise ValueError("A value for `partition` must be specified.")
    return resource
