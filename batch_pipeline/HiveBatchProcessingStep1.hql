// id | dept name | zone | time of day | date of event | duration | text

create external table dasnes_source_from_csv (
id string,
dept_name string,
zone string,
time_of_day string,
date_of_event string,
duration double,
text string
)
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = "\,",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE
  location '/tmp/dasnes-final-project/sample-data/starter-data-final-schema/'; //note that it must point to directory that holds your input data

//now insert all of this into orc

create table dasnes_source_from_csv_as_orc(
id string,
dept_name string,
zone string,
time_of_day string,
date_of_event string,
duration double,
text string
) stored as orc;

insert overwrite table dasnes_source_from_csv_as_orc
select * from dasnes_source_from_csv;

