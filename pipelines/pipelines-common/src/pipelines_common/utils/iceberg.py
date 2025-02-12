from typing import Optional, Tuple

import pyiceberg.exceptions as piexc
from pyiceberg.typedef import Identifier
from pyiceberg.catalog.rest import Catalog, RestCatalog
from pyiceberg.schema import Schema
from pyiceberg.table import Table as IcebergTable

_catalog: Optional[Catalog] = None


# Iceberg
def catalog_get_or_create(name: str, uri: str) -> Catalog:
    """Connect to an iceberg REST catalog on a given uri"""
    global _catalog
    if _catalog is None:
        _catalog = RestCatalog(name, **{"uri": uri})

    return _catalog


def table_identifier(namespace: str, table_name: str) -> Identifier:
    return (namespace, table_name)
