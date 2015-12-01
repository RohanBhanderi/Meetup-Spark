#!/bin/bash

export SPARK_HOME=/opt/mapr/spark/spark-1.4.1

$SPARK_HOME/bin/spark-submit \
	--class SyslogSparkStreamPrinter \
	--master local[2] \
	/user/$USER/LAB2_SUBMISSION/E1/kafka.jar localhost:5181 default cs286 1 \
	> /user/user01/syslog2spark.txt
