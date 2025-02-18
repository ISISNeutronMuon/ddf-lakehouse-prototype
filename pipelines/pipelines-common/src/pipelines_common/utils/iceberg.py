from typing import Dict, Optional

from pyiceberg.typedef import Identifier
from pyiceberg.catalog.rest import Catalog, RestCatalog
from pyiceberg.catalog.sql import SqlCatalog

_catalog: Optional[Catalog] = None


# Iceberg
def catalog_get_or_create(catalog_properties: Dict[str, str]) -> Catalog:
    """Connect to an iceberg catalog on a given uri"""
    global _catalog
    if _catalog is None:
        uri = catalog_properties["uri"]
        if uri.startswith("sqlite:///"):
            _catalog = SqlCatalog(**catalog_properties)
        else:
            _catalog = RestCatalog(**catalog_properties)

    return _catalog


def table_identifier(namespace: str, table_name: str) -> Identifier:
    return (namespace, table_name)
