CREATE EXTERNAL TABLE region (r_regionkey bigint, r_name string, r_comment string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'separatorChar' = '|',
  'quoteChar' = '"',
  'escapeChar' = '\\'
)
TBLPROPERTIES ("skip.header.line.count"="1")
LOCATION 's3://tpc-h-dataset/sf1/region';



CREATE EXTERNAL TABLE IF NOT EXISTS `athena-csv`.`region` (r_regionkey bigint, r_name string, r_comment string) COMMENT "region table"
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'separatorChar' = '|',
  'quoteChar' = '\'',
  'escapeChar' = '\\'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://tpc-h-dataset/sf1/region/'
TBLPROPERTIES ('classification' = 'csv');