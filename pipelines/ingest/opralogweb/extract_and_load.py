from collections.abc import Generator, Iterator, Sequence

import dlt
from dlt.sources import DltResource
from dlt.sources.sql_database import sql_database
from html2text import html2text
import pyarrow as pa

import pipelines_common.cli as cli_utils

# Staging destination
LOADER_FILE_FORMAT = "parquet"


@dlt.transformer(standalone=True)
def html_to_markdown(
    table: pa.Table, *, column_names: Sequence[str]
) -> Iterator[pa.Table]:
    """Given a named set of columns, assuming they are HTML, transform the strings to markdown"""
    for name in column_names:
        if name not in table.column_names:
            continue

        table = table.set_column(
            table.column_names.index(name),
            name,
            pa.array(
                table[name]
                .to_pandas()
                .apply(lambda x: x if x is None else html2text(x))
            ),
        )

    yield table


@dlt.source()
def opralogwebdb() -> Generator[DltResource]:
    """Pull the configured tables from the database backing the Opralog application"""
    tables = dlt.config["sources.sql_database.tables"]
    source = sql_database(
        schema=dlt.config.value,
        backend="pyarrow",
        backend_kwargs={"tz": "UTC"},
        table_names=[table_info["name"] for table_info in tables],
    )
    for table_info in tables:
        resource = getattr(source, table_info["name"])
        resource.apply_hints(
            incremental=dlt.sources.incremental(table_info["incremental_id"])
        )
        if "html_to_markdown_columns" in table_info:
            resource = resource | html_to_markdown(
                column_names=table_info["html_to_markdown_columns"]
            )
        yield resource.with_name(table_info["name"])


# ------------------------------------------------------------------------------

if __name__ == "__main__":
    cli_utils.cli_main(
        pipeline_name="opralogwebdb",
        data=opralogwebdb(),
        default_write_disposition="append",
    )
