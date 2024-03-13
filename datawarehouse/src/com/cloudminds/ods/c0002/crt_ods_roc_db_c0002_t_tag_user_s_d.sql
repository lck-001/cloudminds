-- cdmods.crt_ods_roc_db_c0002_t_tag_user_s_d 标签授权的用户关联表
create external table cdmods.ods_roc_db_c0002_t_tag_user_s_d(
     id string comment '关联ID',
     tag_id string comment '标签信息ID',
     user_id string comment '知识库信息ID',
     exception_flag string comment '例外标识: 0-添加; 1-排除',
     k8s_env_name string comment '环境名称'
)
comment '标签授权的用户关联表roc.t_tag_user'
PARTITIONED BY (dt string COMMENT '日期')
STORED AS parquet;