CREATE EXTERNAL TABLE IF NOT EXISTS cdmdwd.dwd_bigdata_bmd_table_info_i_d(
  table_name string comment '表名称',
  db_name string comment '数据库名称',
  create_time string comment '创建时间',
  update_time string comment '更新时间',
  table_type int comment '1:hive 2 clickhouse',
  table_type_name string comment '1:hive 2 clickhouse',
  num_rows bigint comment '记录数',
  total_size bigint comment '存储大小',
  columns string comment '列信息',
  db_op int comment '1：insert  2:update 3:delete'
) comment 'bigdata表的信息'
PARTITIONED BY (
  dt string comment 'dt分区字段'
)
stored as parquet
location '/data/cdmdwd/dwd_bigdata_bmd_table_info_i_d'
