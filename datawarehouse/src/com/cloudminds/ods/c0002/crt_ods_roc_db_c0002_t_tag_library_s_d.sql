-- cdmods.crt_ods_roc_db_c0002_t_tag_library_s_d 标签的知识库关联表
create external table cdmods.ods_roc_db_c0002_t_tag_library_s_d(
     id string comment '关联ID',
     tag_id string comment '标签信息ID',
     library_id string comment '知识库信息ID',
     k8s_env_name string comment '环境名称'
)
comment '标签的知识库关联表roc.t_tag_library'
PARTITIONED BY (dt string COMMENT '日期')
STORED AS parquet;