CREATE EXTERNAL TABLE IF NOT EXISTS cdmtmp.tmp_bmd_table_info_s_d_tanqiong_20780302(
  table_name string comment '表名称',
  db_name string comment '数据库名称',
  create_time string comment '创建时间',
  update_time string comment '更新时间',
  table_type int comment '1:hive 2 clickhouse',
  table_type_name string comment '1:hive 2 clickhouse',
  num_rows bigint comment '记录数',
  total_size bigint comment '存储大小',
  columns String comment '列信息',
  analyse_type int comment '1:部门 2:主题',
  analyse_type_name string comment '1:部门 2:主题',
  analyse_value string comment 'analyse_type为1就填权限部门，为2填主题名'
) comment 'bigdata表的快照信息'
PARTITIONED BY (
  dt string comment 'dt分区字段'
)
stored as parquet
location '/data/cdmtmp/tmp_bmd_table_info_s_d_tanqiong_20780302'