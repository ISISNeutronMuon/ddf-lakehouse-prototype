# ELT Pipelines

## Prerequisites

### S3 credentials

Create the AWS SDK config & credentials files:

```sh
mkdir $HOME/.aws
cat <<EOF > ~/.aws/config
[default]
endpoint_url = https://s3.echo.stfc.ac.uk
request_checksum_calculation = WHEN_REQUIRED
EOF
cat <<EOF > ~/.aws/credentials
[default]
aws_access_key_id = <insert_access_key_id_here>
aws_secret_access_key = <insert_secret_access_key_here>
EOF
```

### Spark

[PySpark](https://spark.apache.org/docs/latest/api/python/index.html) requires
a local Java installation. Ensure a java executable is on the `PATH`.
Spark currently supports Java <= 17.
