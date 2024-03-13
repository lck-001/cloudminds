create external table cdmdwd.dwd_harix_cmd_navi_charge_i_d (
    guid string comment 'guid',
    robot_id string comment '机器人id',
    robot_type string comment '机器人类型',
    robot_account_id string comment '机器人账户，roc系统中创建的机器人账号',
    service_code string comment '机器人和客服服务码',
    tenant_id string comment '租户id',
    version string comment '当前版本号',
    seq string comment '序列号',
    action string comment '请求的robot行为',
    op_id string comment '',
    status int comment '当前事件状态值',
    status_name string comment '当前事件状态值名称',
    context string comment '上下文',
    msg string comment '对应id信息',
    rod_type string comment '事件类型',
    k8s_svc_name string comment 'k8s smart voice control 名称',
    k8s_env_name string comment 'k8s环境名称',
    event_time string comment '事件发生时时间',
    ext string comment '原始数据'
)
comment '机器人导航充电'
partitioned by(dt string comment 'dt分区字段')
stored as parquet
location '/data/cdmdwd/dwd_harix_cmd_navi_charge_i_d';