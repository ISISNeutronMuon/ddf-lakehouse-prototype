#!/bin/bash
#

start-master.sh -p 7077
SPARK_NO_DAEMONIZE=true start-connect-server.sh --packages org.apache.spark:spark-connect_2.12:3.5.1
# start-worker.sh spark://127.0.0.1:7077
# start-history-server.sh
# start-thriftserver.sh  --driver-java-options "-Dderby.system.home=/tmp/derby"

# # Entrypoint, for example notebook, pyspark or spark-sql
# if [[ $# -gt 0 ]] ; then
#     eval "$1"
# fi
