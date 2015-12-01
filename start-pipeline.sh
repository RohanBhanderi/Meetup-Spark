#!/bin/bash

export KAFKA_HOME=/user/user01/LAB2/E1/KAFKA/kafka_2.10-0.8.2.2

tail -f /var/log/messages | $KAFKA_HOME/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic cs286

