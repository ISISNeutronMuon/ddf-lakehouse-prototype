from pathlib import Path
from typing import Dict

from pyiceberg.catalog import Catalog
from pyiceberg.typedef import Identifier


def create_catalog(catalog_properties: Dict[str, str]) -> Catalog:
    """Connect to an iceberg catalog on a given uri"""
    uri = catalog_properties["uri"]
    if uri.startswith("sqlite:///"):
        from pyiceberg.catalog.sql import SqlCatalog  # noqa: E402

        init_catalog_tables = "false" if Path(uri[10:]).exists() else "true"
        catalog = SqlCatalog(
            init_catalog_tables=init_catalog_tables, **catalog_properties
        )
    else:
        from pyiceberg.catalog.rest import RestCatalog  # noqa: E402

        catalog = RestCatalog(**catalog_properties)

    return catalog


def table_identifier(namespace: str, table_name: str) -> Identifier:
    return (namespace, table_name)
