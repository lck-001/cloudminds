-- ROC 基础配置事实表
CREATE EXTERNAL TABLE IF NOT EXISTS cdmdwd.dwd_roc_cmd_roc_config_i_d(
  rcu_code string comment 'RCU唯一标识rcuId值',
  user_code string comment '用户登录ID',
  robot_code string comment 'robot唯一标识',
  asr_lang string comment 'ASR语言',
  asr_vendor string comment 'ASR供应商',
  sys_volume string comment '设备系统音量',
  tts_vendor string comment 'TTS供应商',
  tts_speaker string comment '发音人',
  tenant_code string comment '租户编码',
  vui_area string comment '交互区域',
  pause_type string comment '断句模式',
  asr_domain string comment '行业引擎',
  agent_id string comment 'AgentID',
  library_id string comment '库信息ID',
  library_name string comment '库名称',
  library_type string comment '库类型',
  library_value string comment '库属性信息(json)',
  update_time string comment '更新时间',
  k8s_env_name string comment '环境名称'
) comment 'ROC 基础配置事实表'
PARTITIONED BY (
  dt string comment 'dt分区字段'
)
stored as parquet
location '/data/cdmdwd/dwd_roc_cmd_roc_config_i_d'