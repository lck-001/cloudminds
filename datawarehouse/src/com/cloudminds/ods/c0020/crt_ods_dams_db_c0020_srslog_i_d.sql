create external table cdmods.ods_dams_db_c0020_srslog_i_d (
     `_id` string comment 'mongo ID',
     name string comment '告警事件名称',
     projectCode string comment '项目code',
     projectName string comment '项目名称',
     `describe` string comment '告警描述,当前状态告警的一个描述,即告警的值',
     `level` int comment '告警级别',
     seatError Boolean comment '是不是坐席发送的数据,研发使用',
     `time` string comment '告警时间',
     videoDuration int comment '视频录制时长',
     tenantid string comment '租户id',
     userid string comment '用户id',
     robotid string comment 'robot id',
     robotname string comment 'robot 名称',
     type string comment 'fixed-巡更点打卡 sudden-突发异常 hardware-事件 unusualalarm-告警',
     eventId int comment '告警事件id,相当于告警事件类型id',
     workOrderId string comment '坐席工单id',
     extraParam string comment '额外参数',
     imageurls array<string> comment '上传的图片地址',
     videourls array<string> comment '上传的视频地址',
     ischeck boolean comment '是否检查',
     detect_type string comment '识别的类型',
     timezone string comment '时区',
     utctime string comment '对应的utc时间',
     edgeai string comment '边缘计算',
     companyCode string comment '公司代码',
     companyName string comment '公司名称'
)
comment 'mongo dams.srslog 机器人告警日志记录,即研发的事件表'
PARTITIONED BY (dt string COMMENT '日期')
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
STORED AS TEXTFILE;