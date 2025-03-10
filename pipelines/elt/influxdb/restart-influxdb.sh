#!/bin/bash
IMAGE_NAME=influxdb:2.7.10
CONTAINER_NAME=influxdb
PORT=8086
INFLUXDB_DATA_DIR=/var/lib/influxdb2
HOST_MOUNT=/mnt/lakehouse_data/staging/influx/influxdbv2/data

if [ -n "$(docker ps | grep $CONTAINER_NAME)" ]; then
  echo "Stopping running container..."
  docker stop "${CONTAINER_NAME}"
  if [ -n "$(docker ps -a | grep $CONTAINER_NAME)" ]; then
    docker rm "${CONTAINER_NAME}"
  fi
fi

echo "Starting new container"
docker run \
  --rm \
  --name ${CONTAINER_NAME} \
  -p $PORT:$PORT \
  -v $HOST_MOUNT:$INFLUXDB_DATA_DIR \
  -d \
  ${IMAGE_NAME}

seconds_start=$SECONDS
while [ "$(curl -s  http://localhost:8086/health | jq --raw-output .status)" !=  "pass" ]; do
  echo "Influx not ready. Waiting..."
  sleep 10
  if [ $(($SECONDS - $seconds_start)) -ge 120 ]; then
    echo "Influx not ready after 120s."
    exit 1
  fi
done
