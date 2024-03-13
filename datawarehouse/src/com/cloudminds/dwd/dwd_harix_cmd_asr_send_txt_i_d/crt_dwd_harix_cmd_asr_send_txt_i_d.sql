-- cdmdwd.dwd_harix_cmd_asr_send_txt_i_d asr发送识别的quest_text到reception skill
create external table cdmdwd.dwd_harix_cmd_asr_send_txt_i_d (
    question_id string comment '请求id',
    robot_id string comment '机器人id',
    rcu_audio string comment '机器人录制的RCU声音文件',
    robot_account_id string comment '机器人账户，roc系统中创建的机器人账号',
    service_code string comment '机器人和客服服务码',
    tenant_id string comment '租户id',
    robot_type string comment '机器人类型',
    rod_type string comment '事件类型',
    qa_flag string comment 'qa问题来源,HI CLOUD',
    question_text string comment '识别出的文本',
    hi_status string comment 'hi座席状态，1-在线，0-不在线',
    language string comment '机器人配置的语言',
    latitude string comment '纬度',
    longitude string comment '经度',
    send_delay int comment '声音识别时候发送时长，毫秒',
    queue_delay int comment '排队时长，毫秒',
    total_delay int comment 'asr send 总时延，毫秒',
    k8s_svc_name string comment 'k8s smart voice control 名称',
    k8s_env_name string comment 'k8s环境名称',
    event_time string comment '事件发生时时间',
    ext string comment '保留的原始数据'
)
comment 'asr发送识别的quest_text到reception skill'
partitioned by(dt string comment 'dt分区字段')
stored as parquet
location '/data/cdmdwd/dwd_harix_cmd_asr_send_txt_i_d';