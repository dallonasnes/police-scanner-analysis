//create hbase table
create 'dasnes_view_as_hbase', 'stats'

//then create it in hive
create external table dasnes_view_as_hbase(
id string,
dept_name string,
zone string,
time_of_day string,
season string,
most_common_words string,
least_common_words string,
sentiment_score_sum bigint,
sentiment_score_total bigint)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,stats:dept_name,stats:zone,stats:time_of_day,stats:season,stats:most_common_words,stats:least_common_words,stats:sentiment_score_sum#b,stats:sentiment_score_total#b,')
TBLPROPERTIES ('hbase.table.name' = 'dasnes_view_as_hbase');

//now need to overwrite computed hive table into hbase view
insert overwrite table dasnes_view_as_hbase
select * from dasnes_view_as_hive;