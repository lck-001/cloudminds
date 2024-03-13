-- cdmdim.dim_cmd_sv_env_a_d sv环境信息
create external table cdmdim.dim_cmd_sv_env_a_d(
     sv_env_id string comment 'SV配置id',
     sv_env_name string comment 'SV配置名称',
     tenant_id string comment '租户id',
     sv_agent_base_url string comment 'SmartVoice平台地址',
     sv_agent_base_url_talk string comment '语义理解接口',
     sv_agent_base_url_confidence string comment '闲聊状态获取接口',
     sv_agent_base_url_set_confidence string comment '闲聊状态设置接口',
     is_built_in int comment '是否内置: 0-否;1-是',
     is_del int comment '是否删除:-1删除;0正常',
     create_time string comment '创建时间',
     update_time string comment '更新时间',
     k8s_env_name string comment '数据来源环境'
)
comment 'sv环境信息'
stored as parquet;