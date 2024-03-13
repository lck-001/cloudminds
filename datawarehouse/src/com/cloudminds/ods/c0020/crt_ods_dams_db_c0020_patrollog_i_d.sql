create external table cdmods.ods_dams_db_c0020_partrollog_i_d (
     `_id` string comment 'mongo ID',
     robotid string comment 'robot id',
     type string comment '备注-无意义',
     projectCode string comment '项目code',
     projectName string comment '项目名称',
     `describe` string comment '任务描述,当前状态执行状态的一个描述',
     title string comment '巡逻事件类型,巡逻事件告警id',
     tenant string comment '租户',
     `level` string comment '备注-无意义',
     pubtime string comment '记录入库时间',
     timezone string comment '时区',
     utctime string comment '对应的utc时间',
     uuid string comment '当前状态唯一的uuid,state_id 即研发的taskid'
)
comment 'mongo dams.patrollog 机器人巡逻日志记录'
PARTITIONED BY (dt string COMMENT '日期')
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
STORED AS TEXTFILE;
