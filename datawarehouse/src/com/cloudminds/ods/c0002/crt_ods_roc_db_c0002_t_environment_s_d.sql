-- cdmods.ods_roc_db_c0002_t_environment_s_d roc系统环境信息
create external table cdmods.ods_roc_db_c0002_t_environment_s_d(
     id string comment '环境信息ID',
     tenant_code string comment '隶属租户code',
     env_name string comment '环境信息名称',
     library_type string comment '库类型',
     env_value string comment '环境具体信息(json)',
     status int comment '环境状态:-1删除;0正常;',
     description string comment '环境简要描述',
     extension string comment '扩展字段',
     built_in int comment '是否内置: 0-否;1-是',
     create_time string comment '环境创建时间',
     update_time string comment '环境更新时间',
     k8s_env_name string comment '数据来源环境'
)
comment 'roc系统环境信息表roc.t_environment'
PARTITIONED BY (dt string COMMENT '日期')
STORED AS parquet;