#!/bin/bash

if [ ! -z "$1" ]; then
  TOPIC=$1
else
  echo "You should pass in topic."
  exit 1
fi

if [ ! -z "$2" ]; then
  DATA_SOURCE_NAME=$2
else
  echo "You should pass in data source name."
  exit 1
fi

KAFKA_HOME=/home/shzq/kafka_2.12-1.1.0
if [ ! -d $KAFKA_HOME ]; then
  echo "Kafka home is not exist."
  exit 1
fi

cd ${KAFKA_HOME}

DATA_HOME=/home/shzq/data
if [ ! -d $DATA_HOME ]; then
  echo "Data home is not exist."
  exit 1
fi

for t in `seq 1 256`
do
  for o in `seq 1 256`
  do
    for p in `seq 1 256`
    do
      echo "Grand Parent: $t"
      echo "Parent: $o"
      echo "Child: $p"
      ./bin/kafka-console-producer.sh --broker-list localhost:9092 --topic ${TOPIC} < ${DATA_HOME}/${DATA_SOURCE_NAME}.txt
      sleep 5
    done
  done
done
