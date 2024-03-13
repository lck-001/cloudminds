create external table cdmdwd.dwd_sv_omd_robot_wakeup_i_d (
    event_type_id string comment '事件编码的值',
    event_name string comment  '事件名称',
    event_time string comment '事件时间',
    model_id string comment '模块id',
    tenant_id string comment '租户id',
    robot_id string comment '机器人id',
    module_name string comment '日志输出模块名称',
    source string comment '数据来源模块名',
    rcu_id string comment 'rcu id',
    robot_type string comment '机器人类型',
    option string comment '扩展字段',
    wake_up_type int comment '唤醒类型 0：人脸:1：语音:2：HA:3：触屏',
    wake_up_type_msg string comment '唤醒类型描述'
)
comment '机器人唤醒事件明细表'
partitioned by (dt string comment '分区时间')
stored as parquet
location '/data/cdmdwd/dwd_sv_omd_robot_wakeup_i_d';