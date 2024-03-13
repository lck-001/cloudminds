--  modify by dave.liang at 2022-04-221
create external table cdmdws.dws_bmd_asr_audio_statistic_i_d (
    bucket String comment 'ceph桶名',
    k8s_env_name String comment '环境名',
    audio_lang String comment '音频语言',
    asr_vendor String  comment 'ASR 厂商',
    asr_domain String comment 'ASR行业引擎',
    is_noise int comment '是否是噪音 1:是噪音，0:非噪音，不存在默认为0',
    tenant_id String comment '租户ID',
    robot_type String comment '机器人类型',
    audio_count bigint comment '音频数量',
    duration_count bigint comment '音频时长（s）',
    size_count bigint comment '音频大小（Byte）',
    tdate string comment '事件日期'
)
comment 'asr音频资产统计表'
    partitioned by(dt string comment 'dt分区字段,事件采集的日期')
    stored as parquet ;