create external table cdmods.ods_crio_db_c0027_srslog_i_d (
     eventId int comment '告警事件id,相当于告警事件类型id',
     detect_type string comment '识别的类型',
     `level` int comment '告警级别',
     seatError Boolean comment '是不是坐席发送的数据,研发使用',
     extraParam string comment '额外参数',
     robotname string comment 'robot 名称',
     robotid string comment 'robot id',
     type string comment 'fixed-巡更点打卡 sudden-突发异常 hardware-事件 unusualalarm-告警',
     userid string comment '用户id',
     ischeck boolean comment '是否检查',
     videoDuration int comment '视频录制时长',
     projectCode string comment '项目code',
     name string comment '告警事件名称',
     tenantid string comment '租户id',
     id string comment 'mongo ID',
    `describe` string comment '告警描述,当前状态告警的一个描述,即告警的值',
    `time` string comment '告警时间',
     workOrderId string comment '坐席工单id',
     projectName string comment '项目名称',
     imageurls string comment '上传的图片地址',
     videourls string comment '上传的视频地址',
     companyCode string comment '公司代码',
     companyName string comment '公司名称',
     k8s_env_name string comment '环境名称'
)
comment 'mongo dams.srslog 机器人告警日志记录'
PARTITIONED BY (dt string COMMENT '日期')
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
STORED AS TEXTFILE;