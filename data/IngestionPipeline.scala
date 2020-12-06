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

