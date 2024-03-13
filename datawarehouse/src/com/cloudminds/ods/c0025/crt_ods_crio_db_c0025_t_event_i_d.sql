-- cdmods.ods_crio_db_c0025_t_event_i_d 机器人事件信息表
create external table cdmods.ods_crio_db_c0025_t_event_i_d(
     id bigint comment '事件ID',
     name string comment '事件名称',
     robot_type string comment '所属机器人类型',
     robot_code string comment '所属机器人编码',
     robot_name string comment '所属机器人名称',
     tenant_id int comment '租户ID',
     tenant_code string comment '租户编码',
     rcu_code string comment 'RCU编码',
     device_id bigint comment '设备ID',
     device_type int comment '设备类型',
     device_code string comment '设备编码',
     type_id int comment '事件类型ID',
     type_name string comment '事件类型名称',
     event_time bigint comment '事件时间',
     level int comment '事件级别',
     details string comment '报事件详情',
     ext_data string comment '扩展字段',
     pic_url string comment '事件图片URL',
     location_info string comment '事件发生时机器人位置信息',
     create_time bigint comment '创建时间',
     read_flag int comment '读取标识：0-未读，1-已读',
     update_time bigint comment '更新时间',
     remark string comment '备注',
     msg_type_id bigint comment '消息类型id',
     branch_tag string comment '分支机构链',
     operate_flag int comment '租户操作标识：0-未操作，1-已操作',
     customer_tag string comment '客户层级',
     bigdata_method string comment 'db操作类型:INSERT;UPDATE;DELETE',
     k8s_env_name string  comment '环境名称'
)
comment 't_event机器人事件信息表'
PARTITIONED BY (dt string COMMENT '日期')
STORED AS parquet
location '/data/cdmods/ods_crio_db_c0025_t_event_i_d';