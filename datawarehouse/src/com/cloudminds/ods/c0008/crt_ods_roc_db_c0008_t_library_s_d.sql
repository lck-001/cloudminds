-- cdmods.ods_roc_db_c0008_t_library_s_d roc系统知识库信息
create external table cdmods.ods_roc_db_c0008_t_library_s_d(
     id string comment '库信息ID',
     tenant_code string comment '隶属租户code',
     library_name string comment '库名称',
     library_type string comment '库类型',
     library_value string comment '库属性信息(json)',
     library_value_ext string comment '库属性扩展信息(json)',
     status int comment '知识库状态:-1删除;0正常;',
     description string comment '库简要描述',
     extension string comment '扩展字段',
     built_in int comment '是否内置: 0-否;1-是',
     create_time string comment '信息创建时间',
     update_time string comment '信息更新时间',
     k8s_env_name string comment '数据来源环境'
)
comment 'roc系统知识库信息表roc.t_library'
PARTITIONED BY (dt string COMMENT '日期')
STORED AS parquet;