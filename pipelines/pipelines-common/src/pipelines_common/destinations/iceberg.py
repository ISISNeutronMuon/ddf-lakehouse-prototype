from typing import Optional

import pyiceberg.exceptions as piexc
from pyiceberg.catalog.rest import Catalog, RestCatalog
from pyiceberg.schema import Schema
from pyiceberg.table import Table as IcebergTable

_catalog: Optional[Catalog] = None


# Iceberg
def get_catalog(name: str, uri: str) -> Catalog:
    """Connect to an iceberg REST catalog on a given uri"""
    global _catalog
    if _catalog is None:
        _catalog = RestCatalog(name, **{"uri": uri})

    return _catalog


def table_qualified_name(namespace: str, table_name: str) -> str:
    return f"{namespace}.{table_name}"


def create_table(
    catalog: Catalog, namespace: str, table_name: str, table_schema: Schema
) -> IcebergTable:
    """Create a table in the catalog. If the schema does not exist it is created too."""
    try:
        catalog.create_namespace(namespace)
    except piexc.NamespaceAlreadyExistsError:
        pass

    return catalog.create_table(
        table_qualified_name(namespace, table_name), schema=table_schema
    )


def drop_table(catalog: Catalog, namespace: str, table_name: str) -> None:
    """Drops the named table from the namespace in the given catalog. It is a noop if the table does not exist."""
    try:
        catalog.drop_table(table_qualified_name(namespace, table_name))
    except piexc.NoSuchTableError:
        pass
