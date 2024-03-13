-- 251环境 cdmods.ods_roc_db_c0008_t_current_robot_config_s_d roc帐号当前生效的配置表
create external table cdmods.ods_roc_db_c0008_t_current_robot_config_s_d(
     id string comment '帐号配置ID',
     user_id string comment '帐号信息ID',
     create_time string comment '创建时间',
     update_time string comment '更新时间',
     status int comment '配置状态：-1-删除 0-启用',
     robot_type int comment '适用的Robot类型: 1-pepper; 2-meta; 3-patrol',
     extension_json string comment '存放配置详细的json',
     k8s_env_name string comment '环境名称'
)
comment '251环境 帐号当前生效的配置表roc.t_current_robot_config'
PARTITIONED BY (dt string COMMENT '日期')
STORED AS parquet;