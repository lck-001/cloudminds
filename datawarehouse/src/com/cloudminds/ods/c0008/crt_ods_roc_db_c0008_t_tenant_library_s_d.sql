-- 251环境 cdmods.ods_roc_db_c0008_t_tenant_library_s_d roc帐号当前生效的配置表
create external table cdmods.ods_roc_db_c0008_t_tenant_library_s_d(
     id string comment '关联ID',
     tenant_code string comment '租户编码',
     library_id string comment '知识库信息ID',
     k8s_env_name string comment '环境名称'
)
comment '251环境 租户的知识库关联表roc.t_tenant_library'
PARTITIONED BY (dt string COMMENT '日期')
STORED AS parquet;