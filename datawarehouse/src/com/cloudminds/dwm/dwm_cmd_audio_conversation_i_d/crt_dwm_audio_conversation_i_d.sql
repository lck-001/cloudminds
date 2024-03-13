create external table cdmdwm.dwm_cmd_audio_conversation_i_d (
    event_type_id string comment '事件编码的值',
    rcu_id string comment 'rcu id',

    robot__robot_type_inner_name string comment '机器人类型',
    robot__robot_id string comment '机器人id',

    tenant__tenant_id string comment '租户id',

    question_id string comment '问题id',
    voice_trigger_time string comment '语音触发事件时间',
    voice_start_play_time string comment '语音播报开始事件时间',
    voice_end_play_time string comment '语音播报结束事件时间'
)
comment '机器人语音播报表'
partitioned by (dt string comment '分区日期')
stored as parquet;
