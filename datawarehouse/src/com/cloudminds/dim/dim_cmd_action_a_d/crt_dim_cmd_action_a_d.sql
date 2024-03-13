create external table cdmdim.dim_cmd_action_a_d (
    action_id string comment 'action id',
    action_name string comment 'action名称',
    action_md string comment 'action与robot_type_inner_name的md5值,后续可以考虑使用md5作为关联',
    robot_type_inner_name string comment '机器人类型',
    robot_intent string comment '机器人意图',
    create_time string comment 'action创建时间',
    update_time string comment 'action更新时间',
    k8s_env_name string comment '环境名'
)
comment 'sv库action表 semantic.dance_mapping,需要intent_label和robot_type_inner_name才能确定'
stored as parquet
location '/data/cdmdim/dim_cmd_action_a_d';