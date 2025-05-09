# pipelines-common

A thin package around [`dlt`](https://dlthub.com) to provide functionality common
to each pipeline.

## Running integration tests

The integration tests for the `pyiceberg` destination require a running Iceberg
rest catalog that is accessible at `http:://localhost:8181`. Currently the
[Lakekeeper minimal docker-compose](https://github.com/lakekeeper/lakekeeper/tree/main/examples/minimal) example provides a convenient
method to spin up an instance that is ready to go.
