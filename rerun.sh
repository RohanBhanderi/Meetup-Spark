#!/bin/bash

export SPARK_HOME=/opt/mapr/spark/spark-1.4.1

$SPARK_HOME/bin/spark-submit \
	--class EventComntsStreamProcessor \
	--master local[2] \
	/home/rohan/meetup-analysis/event-comms/Meetup-Spark/target/event-comms-spark.jar \
	localhost:9092 event_comms \
