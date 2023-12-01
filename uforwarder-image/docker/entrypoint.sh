#!/bin/bash
set -e
echo "Starting uforwarder ..."
PROFILE="${UFORWARDER_PROFILE}"
KAFKA_CONNECT="${UFORWARDER_KAFKA_CONNECT}"
ZOOKEEPER_CONNECT="${UFORWARDER_ZOOKEEPER_CONNECT}"
CONTROLLER_CONNECT="${UFORWARDER_CONTROLLER_CONNECT}"
MEMORY_LIMIT_MB="${UFORWARDER_MEMORY_LIMIT_MB}"

if [ -z "$PROFILE" ]; then
  echo "environment variable UFORWARDER_PROFILE is not set"
  exit 1
fi

if [ "$PROFILE" == "uforwarder-controller" ]; then
  if [ -z "$ZOOKEEPER_CONNECT" ]; then
    echo "environment variable UFORWARDER_ZOOKEEPER_CONNECT is not set for controller"
    exit 1
  fi
fi

if [ -z "$KAFKA_CONNECT" ]; then
  echo "environment variable UFORWARDER_KAFKA_CONNECT is not set"
  exit 1
fi

UFORWARDER_JVM_CONFIG="-Xms${MEMORY_LIMIT_MB}M -Xmx${MEMORY_LIMIT_MB}M"
UFORWARDER_ARGUEMENTS="${PROFILE} \
  --master.zookeeper.zkConnection=${ZOOKEEPER_CONNECT} \
  --master.kafka.admin.bootstrapServers=${KAFKA_CONNECT} \
  --master.kafka.offsetCommitter.bootstrapServers=${KAFKA_CONNECT} \
  --worker.dispatcher.kafka.bootstrapServers=${KAFKA_CONNECT} \
  --worker.fetcher.kafka.bootstrapServers=${KAFKA_CONNECT} \
  --worker.controller.grpc.masterHostPort=${CONTROLLER_CONNECT}"

echo "PROFILE=${PROFILE}"
echo "CONTROLLER_CONNECT=${CONTROLLER_CONNECT}"
echo "KAFKA_CONNECT=${KAFKA_CONNECT}"
echo "ZOOKEEPER_CONNECT=${ZOOKEEPER_CONNECT}"

RUN_COMMAND="java ${UFORWARDER_JVM_CONFIG} -jar bin_main.jar ${UFORWARDER_ARGUEMENTS}"
echo "running ${RUN_COMMAND}"
eval "${RUN_COMMAND}"
