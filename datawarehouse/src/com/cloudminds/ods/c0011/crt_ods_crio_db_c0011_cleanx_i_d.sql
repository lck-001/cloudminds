CREATE EXTERNAL TABLE cdmods.ods_crio_db_c0011_cleanx_i_d(
  id bigint comment "清洁机器人ID",
  tenant_id bigint comment "租户ID",
  tenant_code string comment "租户编码",
  branch_id bigint comment "分支机构ID",
  name string comment "设备名称",
  service_code string comment "服务编码",
  rcu_code string comment "RCU号",
  robot_type string comment "Robot类型",
  robot_code string comment "Robot号",
  roc_user_code string comment "ROC user code",
  manufacturer string comment "厂商",
  firmware_version string comment "固件版本",
  software_version string comment "软件版本",
  model string comment "机器人型号",
  `map` string comment "所在地图",
  create_time string comment "创建时间",
  update_time string comment "修改时间",
  version bigint comment "乐观锁版本号",
  status bigint comment "状态 1：有效 9：删除",
  event_time BIGINT comment '事件发生时间',
  bigdata_method string comment 'db操作类型:INSERT;UPDATE;DELETE',
  k8s_env_name string  comment '环境名称'
) COMMENT '清洁-crss_cxms-cleanx'
PARTITIONED BY (dt string COMMENT '日期')
STORED AS parquet;