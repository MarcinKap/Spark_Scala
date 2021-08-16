spark.sql("show tables").show() 

spark.sql("""CREATE TABLE IF NOT EXISTS `d_time` ( 
    `timestamp` timestamp, 
    `year` int,
    `month` int, 
    `day` int,
    `hour` int) 
ROW FORMAT SERDE 
    'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
 OUTPUTFORMAT 
    'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'""")

spark.sql("""CREATE TABLE IF NOT EXISTS `d_weather` ( 
    `index` bigint, 
    `conditions` string) 
ROW FORMAT SERDE 
    'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
 OUTPUTFORMAT 
    'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'""")

spark.sql("""CREATE TABLE IF NOT EXISTS `d_geography` ( 
    `local_authority_ons_code` string, 
    `local_authority_name` string,
    `region_ons_code` string,
    `region_name` string) 
ROW FORMAT SERDE 
    'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
 OUTPUTFORMAT 
    'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'""")

spark.sql("""CREATE TABLE IF NOT EXISTS `d_roads` ( 
    `id` bigint, 
    `road_category` string,
    `road_type` string) 
ROW FORMAT SERDE 
    'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
 OUTPUTFORMAT 
    'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'""")

spark.sql("""CREATE TABLE IF NOT EXISTS `d_vehicle` ( 
    `id` int, 
    `vehicle_type` string,
    `vehicle_category` string,
    `has_engine` boolean) 
ROW FORMAT SERDE 
    'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
 OUTPUTFORMAT 
    'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'""")

spark.sql(""" CREATE TABLE IF NOT EXISTS `f_facts` (
    `timestamp` timestamp,
    `local_authoirty_ons_code` string,
    `road_category` bigint,
    `vehicle_id` int,
    `weather_id` bigint,
    `vehicle_count` int)
ROW FORMAT SERDE 
    'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
 OUTPUTFORMAT 
    'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'""")
