-- cdmods.ods_crio_db_c0024_t_alarm_i_d 机器人报警信息表
create external table cdmods.ods_crio_db_c0024_t_alarm_i_d(
     id bigint comment '报警信息ID',
     robot_type string comment '所属机器人类型',
     robot_code string comment '所属机器人编码',
     robot_name string comment '所属机器人名称',
     tenant_id int comment '租户ID',
     tenant_code string comment '租户编码',
     rcu_code string comment 'RCU编码',
     device_id bigint comment '设备ID',
     device_type int comment '设备类型',
     device_code string comment '设备编码',
     type_id int comment '报警类型ID',
     type_name string comment '报警类型名称',
     type_cname string comment '报警中文名称',
     event_time bigint comment '事件时间',
     level int comment '报警级别',
     details string comment '报警详情',
     ext_data string comment '扩展字段',
     create_time bigint comment '报警发生时间',
     read_flag int comment '读取标识：0-未读，1-已读',
     update_time bigint comment '更新时间',
     status int comment '状态：0-告警中，1-已消除',
     start_time bigint comment '开始时间',
     end_time bigint comment '结束时间',
     confirm_flag int comment '确认标识：0-手动消除，1-自动解除',
     rdm_alarm_type int comment 'RDM告警类型',
     entity_type string comment '告警实体类型',
     entity_instance string comment '告警实体',
     probable_cause int comment '可能原因',
     specific_problem string comment '特定问题',
     remark string comment '备注',
     msg_type_id bigint comment '消息类型id',
     bigdata_method string comment 'db操作类型:INSERT;UPDATE;DELETE',
     k8s_env_name string  comment '环境名称'
)
comment 't_alarm机器人报警信息表'
PARTITIONED BY (dt string COMMENT '日期')
STORED AS parquet
location '/data/cdmods/ods_crio_db_c0024_t_alarm_i_d';