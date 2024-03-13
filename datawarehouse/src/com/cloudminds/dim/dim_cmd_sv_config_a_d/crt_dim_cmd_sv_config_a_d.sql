-- cdmdim.dim_cmd_sv_config_a_d sv配置信息
create external table cdmdim.dim_cmd_sv_config_a_d(
     sv_config_id string comment 'SV配置id',
     sv_config_name string comment 'SV配置名称',
     robot_id string comment '机器人id',
     sv_agent_id string comment 'NLP库的AgentID',
     sv_agent_name string comment 'NLP库的AgentName',
     is_built_in int comment '是否内置: 0-否;1-是',
     is_del int comment '是否删除:-1删除;0正常',
     create_time string comment '创建时间',
     update_time string comment '更新时间',
     k8s_env_name string comment '数据来源环境'
)
comment 'sv配置信息'
stored as parquet;