CREATE EXTERNAL TABLE cdmods.ods_crio_db_c0013_patrol_i_d(
  id int comment "ID",
  name string comment "名称",
  robotid string comment "ID",
  projectname string comment "项目名称",
  projectcode string comment "CODE",
  companyname string comment "公司名称",
  companycode string comment "公司CODE",
  tenantcode string comment "租户CODE",
  `map` string comment "URL",
  hardware string comment "硬件",
  model string comment "模式",
  memo string comment "备忘",
  `position` string comment "位置",
  maps string comment "map",
  isdetect string comment "是否开启人脸检测:no,yes",
  kafkato int comment "kafka 推送到哪儿,目前只有两个：0 没有推送,1 软通,2金地",
  robotidcopy string comment "一键继承任务的设备ID",
  timezone string comment "时区",
  tpl string comment "监控页模板:default,uav,test",
  media string comment "视频打洞:no,yes",
  event_time BIGINT comment '事件发生时间',
  bigdata_method string comment 'db操作类型:INSERT;UPDATE;DELETE',
  k8s_env_name string  comment '环境名称'
) COMMENT 'gdb_patrol'
PARTITIONED BY (dt string COMMENT '日期')
STORED AS parquet
location '/data/cdmods/ods_crio_db_c0013_patrol_i_d';