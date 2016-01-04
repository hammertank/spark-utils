#!/bin/sh

sep=""

for i in `ls /opt/cloudera/parcels/CDH/lib/hadoop/client/*.jar`
do
        hadoop=$hadoop$sep$i
        sep=:
done


#export HADOOP_HOME="/etc/hadoop"
#export HADOOP_CONF_DIR="/etc/hadoop/conf"

java -cp .:$hadoop:/opt/cloudera/parcels/CDH/jars/spark-assembly-1.3.1-hadoop2.5.0.jar:spark-core-0.0.1-SNAPSHOT-shaded.jar:/etc/hadoop/conf \
my.spark.streaming.ShutdownClient $@
