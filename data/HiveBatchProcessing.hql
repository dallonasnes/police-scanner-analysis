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