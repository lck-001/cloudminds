CREATE EXTERNAL TABLE cdmods.ods_roc_db_c0002_t_robot_supplement_s_d(
	  id string COMMENT 'robot_ID',
	  tenant_code string COMMENT '隶属租户code',
	  robot_code string COMMENT 'robot唯一标识值',
	  robot_name string COMMENT 'robot名称',
	  robot_type int COMMENT 'robot类型: 1-Pepper; 2-META; 3-Patrol; 4-Ginger; 5-NUC; 6-PAD',
	  online_flag int COMMENT '在线状态: 0-不在线; 1-在线',
	  service_flag int COMMENT '0:初始值;1: AI服务中;2:人工服务中;3:服务中断',
	  service_seat string COMMENT '服务坐席',
	  longitude double COMMENT '经度',
	  latitude double COMMENT '纬度',
	  regist_time string COMMENT '注册时间',
	  update_time string COMMENT 'robot信息更新时间',
	  description string COMMENT '描述信息',
	  k8s_env_name string COMMENT '环境名称'
)comment '机器人供应商表'
PARTITIONED BY ( dt string COMMENT '日期')
STORED AS parquet;