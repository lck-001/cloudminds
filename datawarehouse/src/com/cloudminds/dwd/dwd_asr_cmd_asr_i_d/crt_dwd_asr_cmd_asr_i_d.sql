-- asrData 音频识别事实表
CREATE EXTERNAL TABLE IF NOT EXISTS cdmdwd.dwd_asr_cmd_asr_i_d(
  object_id string comment 'ceph object_id',
  voice_file_name string comment '录制的声音文件名',
  bucket String comment 'ceph桶名',
  audio_file_url string comment '录制的声音文件url',
  audio_duration int comment '录制的声音时长',
  audio_size int comment '录制的声音文件大小',
  audio_rate int comment '音频频率',
  audio_lang String comment '音频语言',
  audio_format String comment 'PCM',
  audio_channel int comment '音频轨道',
  asr_type String comment 'ASR类型',
  asr_vendor String  comment 'ASR 厂商',
  asr_service_version String  comment '服务版本号',
  asr_domain String comment 'ASR行业引擎',
  asr_text String comment '音频识别的文本',
  question_id String  comment '串联对话的id',
  is_noise int comment '是否是噪音 1:是噪音，0:非噪音，不存在默认为0',
  service_code String comment '服务代码',
  robot_id String comment '机器人ID',
  robot_account_id String comment 'roc用户ID（sv 的session_id）',
  robot_type String comment '机器人类型',
  tenant_id String comment '租户ID',
  k8s_env_name String comment '环境名',
  agent_id String comment 'Sv 的 agentID',
  event_time string comment '声音文件创建时间'
) comment 'ASR音频数据事实表'
PARTITIONED BY (
  dt string comment 'dt分区字段'
)
stored as parquet
location '/data/cdmdwd/dwd_asr_cmd_asr_i_d'
