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

//then create the hive-only source table we'll use

//NOTE that time_of_date in this table is in 6 hr batches of a day
//morn, aftrn, night, latenight
create table dasnes_view_as_hive(
id string,
dept_name string,
zone string,
time_of_day string,
season string,
most_common_words string,
least_common_words string,
sentiment_score_sum bigint,
sentiment_score_total bigint
);

// now that this empty table is created, we can populate it with spark
// by running a spark-submit job on the batch data


