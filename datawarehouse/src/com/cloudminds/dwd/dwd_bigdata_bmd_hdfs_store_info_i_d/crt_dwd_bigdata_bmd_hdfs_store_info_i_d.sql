CREATE EXTERNAL TABLE IF NOT EXISTS cdmdwd.dwd_bigdata_bmd_hdfs_store_info_i_d(
  creator_name string comment '文件创建者',
  file_path string comment '文件路径',
  create_time string comment '创建时间',
  update_time string comment '更新时间',
  file_type int comment '1:文件 2 目录',
  file_type_name string comment '1:文件 2 目录',
  file_size bigint comment '文件大小',
  analyse_type int comment '1:部门 2:主题',
  analyse_type_name string comment '1:部门 2:主题',
  analyse_value string comment 'analyse_type为1就填权限部门，为2填主题名'
) comment 'hdfs文件存储信息'
PARTITIONED BY (
  dt string comment 'dt分区字段'
)
stored as parquet
location '/data/cdmdwd/dwd_bigdata_bmd_hdfs_store_info_i_d'