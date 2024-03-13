CREATE EXTERNAL TABLE IF NOT EXISTS cdmdws.dws_bmd_table_info_i_d(
  table_type_name string comment '表类型名称',
  analyse_type_name string comment '分析类型名称',
  analyse_value string comment '分析类型值',
  db_count int comment '数据库数量',
  table_count int comment '表数量',
  columns_count int comment '列数量',
  num_rows bigint comment '记录数',
  total_size bigint comment '存储大小,存储大小,单位byte'
) comment 'bigdata表的信息'
PARTITIONED BY (
  dt string comment 'dt分区字段'
)
stored as parquet
location '/data/cdmdws/dws_bmd_table_info_i_d'