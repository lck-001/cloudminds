create external table cdmads.ads_cmd_sv_hitlog_i_d (
    sv_agent_id string comment 'NLP库的AgentID',
    robot_id string comment '机器人id标识，既是robot_code',
    intent_name string comment '意图',
    tag_request string comment '意图热门问题',
    cnt int comment '问题数',
    k8s_env_name string comment '环境'
)
comment 'sv hitlog相同intent下request聚合表'
partitioned by(dt string comment '分区日期')
stored as parquet;