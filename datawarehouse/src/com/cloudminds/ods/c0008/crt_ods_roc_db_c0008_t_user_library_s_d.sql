-- 251环境 cdmods.ods_roc_db_c0008_t_user_library_s_d roc帐号当前生效的配置表
create external table cdmods.ods_roc_db_c0008_t_user_library_s_d(
     id string comment '关联ID',
     user_id string comment '用户信息ID',
     library_id string comment '知识库信息ID',
     k8s_env_name string comment '环境名称'
)
comment '251环境 用户的知识库关联表roc.t_user_library'
PARTITIONED BY (dt string COMMENT '日期')
STORED AS parquet;