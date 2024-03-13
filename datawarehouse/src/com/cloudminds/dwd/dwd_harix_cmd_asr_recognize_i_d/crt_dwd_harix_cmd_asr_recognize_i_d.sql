-- cdmdwd.dwd_harix_cmd_asr_recognize_i_d asr识别录制的声音
create external table cdmdwd.dwd_harix_cmd_asr_recognize_i_d (
    question_id string comment '请求id',
    robot_id string comment '机器人id',
    rcu_audio string comment '机器人录制的RCU声音文件',
    is_noise int comment '是否是噪音 1-表示噪音，0-表示不是噪音',
    robot_account_id string comment '机器人账户，roc系统中创建的机器人账号',
    version string comment '当前版本号',
    sid string comment '当前会话的session id',
    service_code string comment '机器人和客服服务码',
    tenant_id string comment '租户id',
    app_type string comment '机器人RCU中安装的app类型',
    robot_type string comment '机器人类型',
    rod_type string comment '事件类型',
    audio_record_type string comment 'audio声音录制类型 STREAMING',
    qa_flag string comment 'qa问题来源,HI CLOUD',
    duration int comment '对话持续时间',
    dialect string comment '本地方言',
    audio_sample_rate string comment '音频采样率',
    asr_vendor string comment 'asr提供商',
    format string comment '格式',
    channel int comment '频道',
    language string comment '机器人配置的语言',
    vpr_id string comment '声纹id',
    question_text string comment '识别出的文本',
    audio_length int comment '声音文件大小，字节',
    asr_vendor_delay int comment 'asr vendor 供应商延迟，毫秒',
    rcc_delay int comment 'rcc 延迟，毫秒',
    forward_delay int comment '转发延迟',
    sv_agent_id string comment 'smart voice 的agent id',
    k8s_svc_name string comment 'k8s smart voice control 名称',
    k8s_env_name string comment 'k8s环境名称',
    event_time string comment '事件发生时时间',
    ext string comment '保留的原始数据'
)
comment 'asr识别录制的声音'
partitioned by(dt string comment 'dt分区字段')
stored as parquet
location '/data/cdmdwd/dwd_harix_cmd_asr_recognize_i_d';