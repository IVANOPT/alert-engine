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

if [ ! -z "$3" ]; then
  BROKER_LIST=$3
else
  echo "You should pass in broker list."
  exit 1
fi

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
      kafka-console-producer --broker-list ${BROKER_LIST} --topic ${TOPIC} < ${DATA_HOME}/${DATA_SOURCE_NAME}.txt
      sleep 5
    done
  done
done
