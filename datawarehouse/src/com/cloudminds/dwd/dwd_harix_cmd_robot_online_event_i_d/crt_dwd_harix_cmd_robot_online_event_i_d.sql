-- cdmdwd.dwd_harix_cmd_robot_online_event_i_d robot在线事件
create external table cdmdwd.dwd_harix_cmd_robot_online_event_i_d (
     service_code string comment '机器人和客服服务码',
     switch_id string comment '',
     switch_data string comment '',
     err_msg string comment '错误信息',
     err_code int comment '错误编码',
     robot_id string comment '机器人id',
     rcu_id string comment 'rcu id',
     robot_account_id string comment '机器人账户，roc系统中创建的机器人账号',
     version string comment '版本号',
     sid string comment '会话id',
     tenant_id string comment '租户id',
     robot_type_inner_name string comment '机器人类型',
     rod_type string comment '事件类型:robotOnlineTick在线（十分钟持续在线发一次）robotConnect上线 robotAbnormalDisconnect异常下线',
     event_time string comment '事件发生时时间',
     k8s_svc_name string comment 'k8s smart voice control 名称',
     k8s_env_name string comment 'k8s环境名称',
     ext string comment '保留的原始数据'
)
comment 'robot在线事件'
partitioned by(dt string comment 'dt分区字段')
stored as parquet
location '/data/cdmdwd/dwd_harix_cmd_robot_online_event_i_d';