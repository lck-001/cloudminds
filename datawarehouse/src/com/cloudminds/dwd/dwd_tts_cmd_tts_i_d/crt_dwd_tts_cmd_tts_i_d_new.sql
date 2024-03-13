-------------- tts 音频事实表-----------------
CREATE EXTERNAL TABLE IF NOT EXISTS cdmdwd.dwd_tts_cmd_tts_i_d(
    tenant_id string comment '租户id',
    robot_type string comment '机器类型',
    user_id string comment '用户id',
    robot_id string comment '机器id',
    guid string comment 'guid',
    service_code string comment '服务编码',
    vendor_delay int comment '音频时延',
    volume string comment '音量',
    gender string comment '性别',
    speaker string comment '扬声器/说话者',
    is_cache string comment '是否有缓存',
    `language` string comment '音频语言',
    audio string comment '音频',
    pitch string comment '音高',
    text string comment '音频文本',
    tts_vendor string comment 'tts 供应商',
    speed string comment '速度',
    model_id string comment '模型id',
    root_guid string comment 'GUID',
    version string comment '版本',
    event_time string comment '事件时间',
    event_type_id string comment '事件类型id',
    k8s_env_name string comment '环境名'
) comment 'TTS音频数据事实表'
PARTITIONED BY (dt string comment 'dt分区字段')
stored as parquet
location '/data/cdmdwd/dwd_tts_cmd_tts_i_d'
