CREATE EXTERNAL TABLE IF NOT EXISTS cdmdws.dws_bmd_hdfs_store_info_i_d(
  analyse_type_name string comment '分析类型名称',
  analyse_value string comment '分析类型值',
  file_count bigint comment '文件数量',
  total_size bigint comment '存储大小,单位byte'
) comment 'hdfs存储信息'
PARTITIONED BY (
  dt string comment 'dt分区字段'
)
stored as parquet
location '/data/cdmdws/dws_bmd_hdfs_store_info_i_d'