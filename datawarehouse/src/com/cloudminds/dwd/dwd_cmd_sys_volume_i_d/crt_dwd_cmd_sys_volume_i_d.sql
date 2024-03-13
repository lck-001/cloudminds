-- 机器人系统音量数据事实表
CREATE EXTERNAL TABLE IF NOT EXISTS cdmdwd.dwd_cmd_sys_volume_i_d(
  service_code string comment '服务代码',
  robot_id string     comment '机器人ID',
  robot_account_id string      comment 'user id',
  robot_type string   comment '机器人类型',
  tenant_id string    comment '租户ID',
  sys_volume string   comment '系统音量',
  event_time string   comment '事件发生的时间',
  k8s_env_name string comment '环境名'
) comment '机器人系统音量数据事实表'
PARTITIONED BY (
  dt string comment 'dt分区字段'
)
stored as parquet
location '/data/cdmdwd/dwd_cmd_sys_volume_i_d'