CREATE external TABLE `cdmods.ods_crio_db_c0015_t_event_i_d` (
    `id` string COMMENT '事件ID',
    `name` string COMMENT '事件名称',
    `robot_type` string COMMENT '所属机器人类型',
    `robot_code` string COMMENT '所属机器人编码',
    `robot_name` string COMMENT '所属机器人名称',
    `tenant_id` int COMMENT '租户ID',
    `tenant_code` string COMMENT '租户编码',
    `rcu_code` string COMMENT 'RCU编码',
    `device_id` string COMMENT '设备ID',
    `device_type` int COMMENT '设备类型',
    `device_code` string COMMENT '设备编码',
    `type_id` int COMMENT '事件类型ID',
    `type_name` string COMMENT '事件类型名称',
    `event_time` string COMMENT '事件时间',
    `level` int COMMENT '事件级别',
    `details` string COMMENT '事件详情',
    `ext_data` string COMMENT '扩展字段',
    `pic_url` string COMMENT '事件图片URL',
    `location_info` string COMMENT '事件发生时机器人位置信息',
    `create_time` string COMMENT '创建时间',
    `read_flag` int COMMENT '读取标识：0-未读，1-已读',
    `update_time` string COMMENT '更新时间',
    `remark` string COMMENT '备注',
    `msg_type_id` string COMMENT '消息类型id',
    `bigdata_method` string comment 'binlog类型'
) 
comment 'crss event表'
PARTITIONED by(dt string comment '分区时间')
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'STORED AS TEXTFILE;