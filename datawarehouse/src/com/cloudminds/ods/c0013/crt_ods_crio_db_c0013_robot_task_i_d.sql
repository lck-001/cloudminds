CREATE EXTERNAL TABLE cdmods.ods_crio_db_c0013_robot_task_i_d(
  id int comment "id",
  name string comment "名称",
  starttime string comment "开始时间",
  endtime string comment "结束时间",
  times int comment "次数",
  pathid int comment "路径id",
  robotid string comment "机器id",
  pubtime string comment "",
  taskpath string comment "任务路径",
  isdel string comment "no,yes",
  type string comment "loop,fixed",
  mapid int comment "地图id",
  mapname string comment "地图名称",
  status string comment "任务状态：false,true",
  taskstatus string comment "执行过changed，未执行为空",
  taskstatusfan string comment "roc返回任务执行状态默认为空 ，未开始,执行中,暂停,结束",
  event_time BIGINT comment '事件发生时间',
  bigdata_method string comment 'db操作类型:INSERT;UPDATE;DELETE',
  k8s_env_name string  comment '环境名称'
) COMMENT '安保—gdb-robot_task'
PARTITIONED BY (dt string COMMENT '日期')
STORED AS parquet
location '/data/cdmods/ods_crio_db_c0013_robot_task_i_d';