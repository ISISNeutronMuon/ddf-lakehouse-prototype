#!/bin/bash
#

start-master.sh -p 7077
SPARK_NO_DAEMONIZE=true start-connect-server.sh --packages org.apache.spark:spark-connect_2.12:3.5.1
