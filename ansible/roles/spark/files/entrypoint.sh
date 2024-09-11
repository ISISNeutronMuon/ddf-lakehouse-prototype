#!/bin/bash
#
SPARK_CONNECT_VERSION=3.5.1

start-master.sh -p 7077
SPARK_NO_DAEMONIZE=true start-connect-server.sh --packages org.apache.spark:spark-connect_${SCALA_VERSION}:${SPARK_CONNECT_VERSION}
