from pathlib import Path
import threading
from typing import Dict, Optional

from pyiceberg.catalog import Catalog
from pyiceberg.typedef import Identifier


_catalog: Optional[Catalog] = None
_catalog_lock = threading.Lock()


def catalog_get_or_create(catalog_properties: Dict[str, str]) -> Catalog:
    """Connect to an iceberg catalog on a given uri"""
    global _catalog, _catalog_lock
    if _catalog is not None:
        return _catalog

    with _catalog_lock as lock:
        if _catalog is None:
            uri = catalog_properties["uri"]
            if uri.startswith("sqlite:///"):
                from pyiceberg.catalog.sql import SqlCatalog  # noqa: E402

                init_catalog_tables = "false" if Path(uri[10:]).exists() else "true"
                _catalog = SqlCatalog(
                    init_catalog_tables=init_catalog_tables, **catalog_properties
                )
            else:
                from pyiceberg.catalog.rest import RestCatalog  # noqa: E402

                _catalog = RestCatalog(**catalog_properties)

    return _catalog


def table_identifier(namespace: str, table_name: str) -> Identifier:
    return (namespace, table_name)
