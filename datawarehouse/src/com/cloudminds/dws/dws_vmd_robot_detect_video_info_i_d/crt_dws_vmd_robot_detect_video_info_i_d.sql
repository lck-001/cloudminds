CREATE EXTERNAL TABLE IF NOT EXISTS cdmdws.dws_vmd_robot_detect_video_info_i_d(
  type_name string comment '图片类型名称',
  k8s_env_name string comment '数据来源环境',
  tenant_name string comment '租户名称',
  tenant_type_name string comment '租户类型名称',
  tenant_industry_name string comment '租户行业名称',
  video_count bigint comment '图片数量'
) comment 'bigdata表的信息'
PARTITIONED BY (
  dt string comment 'dt分区字段'
)
stored as parquet
location '/data/cdmdws/dws_vmd_robot_detect_video_info_i_d'