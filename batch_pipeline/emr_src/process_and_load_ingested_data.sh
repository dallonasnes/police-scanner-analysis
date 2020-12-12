#!/bin/bash

echo "loading all source truth data into hive as orc table"
beeline -u jdbc:hive2://localhost:10000/default -n hadoop -d org.apache.hive.jdbc.HiveDriver -f HiveBatchProcessingStep1.hql
echo "finished load"

echo "running spark pipeline to build analysis table and load into hive"
# TODO: convert BatchViewsPipeline.scala into jar
spark-submit --master local[2] --driver-java-options "-Dlog4j.configuration=file:///home/hadoop/ss.log4j.properties" --class BatchViewsPipeline uber-batch-views-pipeline-1.0-SNAPSHOT.jar
echo "finished spark pipeline"

echo "now load analysis table from hive into hbase"
beeline -u jdbc:hive2://localhost:10000/default -n hadoop -d org.apache.hive.jdbc.HiveDriver -f HiveBatchProcessingStep2.hql
echo "ingestion complete"