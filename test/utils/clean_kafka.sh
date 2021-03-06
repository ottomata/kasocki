#!/bin/bash

check_kafka_home() {
    if [ "x$KAFKA_HOME" = "x" ]; then
      echo "Please set KAFKA_HOME env variable to the kafka install directory"
      exit 1
    fi
}

if [ -z "${KAFKA_TOPICS_CMD}" ]; then
    check_kafka_home
    KAFKA_TOPICS_CMD="${KAFKA_HOME}/bin/kafka-topics.sh"
fi
if [ -z "${KAFKA_CONSOLE_PRODUCER_CMD}" ]; then
    check_kafka_home
    KAFKA_CONSOLE_PRODUCER_CMD="${KAFKA_HOME}/bin/kafka-console-producer.sh"
fi

dropTopics ( ) {
  if [ "$#" -eq 1 ]
  then
    PATTERN=$1
    echo "looking for topics named '*${PATTERN}*'..."
    TOPICS=`${KAFKA_TOPICS_CMD} --zookeeper localhost:2181 --list \
    	| grep ${PATTERN} \
    	| grep -v 'marked for deletion$'`
    for TOPIC in ${TOPICS}
    do
      echo "Dropping topic ${TOPIC}"
      ${KAFKA_TOPICS_CMD} --zookeeper localhost:2181 --delete --topic ${TOPIC} > /dev/null
    done
  fi
}

createTopic ( ) {
    echo "Creating topic ${1}"
    ${KAFKA_TOPICS_CMD} --create \
        --zookeeper 127.0.0.1:2181             \
        --partitions 1                         \
        --replication-factor 1                 \
        --topic $1 > /dev/null
}

produceTestData ( ) {
    echo "Producing ${2} into topic ${1}"
    ${KAFKA_CONSOLE_PRODUCER_CMD} --broker-list localhost:9092 --topic ${1} < ${2}
}

check ( ) {
  PORT=$1
  SERVICE_NAME=$2
  if [ `nc localhost ${PORT} < /dev/null; echo $?` != 0 ]; then
    echo "${SERVICE_NAME} not running, start it first"
    exit 1
  fi
}

check 2181 "Zookeeper"
check 9092 "Kafka"
dropTopics "kasocki_test_"
sleep 5

#  TODO: 0 index these topic names and data
createTopic kasocki_test_01
createTopic kasocki_test_02
createTopic kasocki_test_03
createTopic kasocki_test_04

produceTestData kasocki_test_01 $(dirname $0)/test_data1.json
produceTestData kasocki_test_02 $(dirname $0)/test_data2.json
produceTestData kasocki_test_03 $(dirname $0)/test_data3.json
