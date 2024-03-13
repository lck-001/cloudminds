CREATE EXTERNAL TABLE IF NOT EXISTS cdmdws.dws_cmd_sv_statistic_a_manual(
  cnt int comment '音频数量',
  duration bigint comment '音频时长',
  file_size bigint comment '存储大小',
  duration_scop string comment '音频时长范围',
  svsa0002001001 string comment '用途',
  svsa0001002001 string comment 'speakerid',
  svsa0001002002 string comment '性别',
  svsa0003002001 string comment '数据集'
) comment '音频数据统计'
stored as parquet
location '/data/cdmdws/dws_cmd_sv_statistic_a_manual'