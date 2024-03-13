create external table cdmdwm.dwm_omd_robot_wakeup_i_d (
    event_time string comment '唤醒事件时间',
    tenant__tenant_id string comment '租户id',
    robot__robot_id string comment '机器人id',
    rcu_id string comment 'rcu id',
    robot__robot_type_inner_name string comment '机器人类型',
    wake_up_type int comment '唤醒类型 0：人脸:1：语音:2：HA:3：触屏',
    wake_up_type_msg string comment '唤醒类型描述',
    idle_time string comment '唤醒后再次进入idle的时间'
)
comment '机器人唤醒记录表'
partitioned by (dt string comment '分区日期')
stored as parquet
