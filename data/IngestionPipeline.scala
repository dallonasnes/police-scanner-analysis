import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.ml.feature.{HashingTF, Tokenizer, CountVectorizer, RegexTokenizer, StopWordsRemover, IDF}
import org.apache.spark.sql.SaveMode

import scala.math.pow
import scala.collection.mutable



val spark = SparkSession.builder.getOrCreate()
//read tsv of zone and text into a dataframe
val text_df = spark.read.option("sep", "\t").csv("hdfs:///tmp/dasnes-final-project/sample-data/test_kvp_combined.tsv")
//write that out as parquet
text_df.write.parquet("hdfs:///tmp/dasnes-final-project/sample-data/test_kvp_combined_parquet/")


//here we build a hive table out of the dataframe
text_df.write.mode(SaveMode.Overwrite).saveAsTable("dasnes_proj_basic_testing2")

//and that worked! can verify with this hql : select * from dasnes_proj_basic_test1 limit 2

/*
Now reading parquet files into a db and then overwriting our new table

first create the table in hbase with
create 'dasnes_proj_basic_testing2', 'score'

then create it in hive with


then run spark shell including the hive hbase handler jar
spark-shell --conf spark.hadoop.metastore.catalodefault=hive --driver-class-path /usr/lib/hive/lib/hive-hbase-handler.jar
NOTE: THIS DOESN'T WORK IN SPARK YET
*/
spark-shell --conf spark.hadoop.metastore.catalog.default=hive
val text = spark.read.parquet("hdfs:///tmp/dasnes-final-project/sample-data/test_kvp_combined_parquet/")
text.write.mode(SaveMode.Overwrite).saveAsTable("dasnes_proj_basic_sample_d")


/*


--------------------------
below is how we loaded starter data into an hbase managed table
started by using python to turn text into csv where the first column is zone name + timestamp
then loaded the csv into a hive table
then created a table as orc, and insert overwrote that from the csv's hive table
then created a table managed by hbase, and insert overwrote that from the orc hive table
*/
//TODO: need to figure out how to write to a hive table in spark that's managed (or at least appears in) hbase

create external table dasnes_test_csv (
zone_timestamp string,
text string
)
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = "\,",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE
  location '/tmp/dasnes-final-project/sample-data/zones/'; //note that it must point to directory that holds your input data

//now insert all of this into orc

create table dasnes_test_csv_orc(
zone_timestamp string,
text string
) stored as orc;

insert overwrite table dasnes_test_csv_orc
select * from dasnes_test_csv;

//then create the table we'll use

create 'dasnes_proj_csv_as_hbase', 'score'

//then create it in hive
create external table dasnes_proj_csv_as_hbase(
zone_timestamp string,
text string)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,score:text')
TBLPROPERTIES ('hbase.table.name' = 'dasnes_proj_csv_as_hbase');


//then insert overwrite the hive table linked to hbase from the orc hive table
insert overwrite table dasnes_proj_csv_as_hbase
select * from dasnes_test_csv_orc;


/*

NEXT TODO:
//to expose port 8070 from hbase connection:
ssh -i dasnes -L 8070:ec2-52-15-169-10.us-east-2.compute.amazonaws.com:8070 hadoop@ec2-52-15-169-10.us-east-2.compute.amazonaws.com
web client should take in a zone and display all rows + timestamps for that row
*/

/*

setup a new kafka topic for lambda function to write to
"topic_dasnes_transcription_finished"

created the topic by going to cluster and
cd /home/hadoop/kafka_2.12-2.2.1/bin/

then
(can abbreviate connection string with just: Kafka-Zookeepers)
./kafka-topics.sh --create --zookeeper z-2.mpcs53014-kafka.fwx2ly.c4.kafka.us-east-2.amazonaws.com:2181,z-3.mpcs53014-kafka.fwx2ly.c4.kafka.us-east-2.amazonaws.com:2181,z-1.mpcs53014-kafka.fwx2ly.c4.kafka.us-east-2.amazonaws.com:2181 --replication-factor 1 --partitions 1 --topic topic_dasnes_transcription_finished


NEW TOPICS FOR WEB APP
topic_dasnes_web_upload_with_audio
topic_dasnes_web_upload_no_audio


and verify it is there by running:

./kafka-topics.sh --list --zookeeper z-2.mpcs53014-kafka.fwx2ly.c4.kafka.us-east-2.amazonaws.com:2181,z-3.mpcs53014-kafka.fwx2ly.c4.kafka.us-east-2.amazonaws.com:2181,z-1.mpcs53014-kafka.fwx2ly.c4.kafka.us-east-2.amazonaws.com:2181 | grep dasnes


I can produce to the topic with 
./kafka-console-producer.sh --broker-list b-2.mpcs53014-kafka.fwx2ly.c4.kafka.us-east-2.amazonaws.com:9092,b-1.mpcs53014-kafka.fwx2ly.c4.kafka.us-east-2.amazonaws.com:9092 --topic topic_dasnes_transcription_finished

And I can consume from that topic with:
./kafka-console-consumer.sh --bootstrap-server b-2.mpcs53014-kafka.fwx2ly.c4.kafka.us-east-2.amazonaws.com:9092,b-1.mpcs53014-kafka.fwx2ly.c4.kafka.us-east-2.amazonaws.com:9092 --topic topic_dasnes_transcription_finished --from-beginning


Run the spark submit job as such:
spark-submit --master local[2] --driver-java-options "-Dlog4j.configuration=file:///home/hadoop/ss.log4j.properties" --class StreamWeather uber-kafka-consumer-2-1.0-SNAPSHOT.jar b-1.mpcs53014-kafka.fwx2ly.c4.kafka.us-east-2.amazonaws.com:9092,b-2.mpcs53014-kafka.fwx2ly.c4.kafka.us-east-2.amazonaws.com:9092


test by:
enter this in the producer: {"zone_timestamp": "12345", "text": "my text"}
and can check in hbase by: get 'dasnes_proj_csv_as_hbase', '12345'

can test node on load balanced servers here: 
http://mpcs53014-loadbalancer-217964685.us-east-2.elb.amazonaws.com:3005/submit-weather.html

*/


/*
update the python code in a lambda by running:
aws lambda update-function-code --function-name dasnes-write-uri-to-kafka-topic-on-transcribe-finish --zip-file fileb://my-deployment-package.zip

*/

/*
starting with just one hive table:

id | dept name | zone | time of day | date of event | duration | text

I simulated source data to match this schema, then moved it into AWS
From there, I ssh'd into the cluster and ran this command to put that csv into hdfs
#first create the new dir
hdfs dfs -mkdir /tmp/dasnes-final-project/sample-data/starter-data-final-schema/
#then copy the data over
aws s3 cp s3://dasnes-mpcs53014/starter_data_final_schema.csv /dev/stdout | hdfs dfs -put - /tmp/dasnes-final-project/sample-data/starter-data/
*/




