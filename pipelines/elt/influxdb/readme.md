# InfluxDB ELT

## Useful commands

Find the total number of channels currently in the destination bucket.

```shell
let total_count=0;
for name in $(echo $vsystem_schemas|xargs); do
    _count=$(s3cmd ls s3://prod-lakehouse-source-accelerator-influxdb/machinestate/$name/ | wc -l);
    echo $name $((_count));
    total_count=$(($total_count+$_count));
done;
echo;
echo "Total channels read $total_count"
```
