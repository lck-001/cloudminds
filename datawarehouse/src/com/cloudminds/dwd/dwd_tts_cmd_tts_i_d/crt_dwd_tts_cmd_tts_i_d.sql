-- ttsData 音频事实表
CREATE EXTERNAL TABLE IF NOT EXISTS cdmdwd.dwd_tts_cmd_tts_i_d(
  event_time string   comment '事件发生的时间',
  tenant_id string    comment '租户ID',
  robot_id string     comment '机器人ID',
  robot_account_id string      comment 'user id',
  robot_type string   comment '机器人类型',
  question_id string  comment '串联端到端对话',
  service_code string comment '服务代码',
  version string      comment 'tts skill 版本号',
  tts_vendor string   comment '供应商',
  sys_volume string   comment '系统音量',
  volume string       comment '音量',
  speed string        comment '语速',
  pitch string        comment '音调',
  speaker string      comment '发声人',
  lang string         comment '音频语言',
  audio_url string    comment '合成音频地址',
  tts_text string     comment '音频文本',
  vendor_delay string comment 'vendor 时延',
  is_cache int     comment '是否名字缓存,0:无缓存，1:有缓存',
  k8s_env_name string comment '环境名'
) comment 'TTS音频数据事实表'
PARTITIONED BY (
  dt string comment 'dt分区字段'
)
stored as parquet
location '/data/cdmdwd/dwd_tts_cmd_tts_i_d'
