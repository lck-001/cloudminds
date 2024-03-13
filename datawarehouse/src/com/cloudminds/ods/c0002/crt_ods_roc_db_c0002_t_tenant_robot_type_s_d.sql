CREATE EXTERNAL TABLE cdmods.ods_roc_db_c0002_t_tenant_robot_type_s_d(
	  id string COMMENT '关联ID',
	  tenant_code string COMMENT '租户编码',
	  robot_type int COMMENT '机器人类型: 1-Pepper; 2-META; 3-Patrol; 4-Ginger; 5-NUC; 6-PAD',
	  k8s_env_name string COMMENT '环境名称'
)comment '租户机器人类型表'
PARTITIONED BY ( dt string comment '日期')
STORED AS parquet;