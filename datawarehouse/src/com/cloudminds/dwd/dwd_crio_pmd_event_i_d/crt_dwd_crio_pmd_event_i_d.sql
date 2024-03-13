create external table cdmdwd.dwd_crio_pmd_event_i_d (
    event_id string COMMENT '事件ID',
    event_name string COMMENT '事件名称',
    robot_type_inner_name string COMMENT '所属机器人类型',
    -- robot_code
    robot_id string COMMENT '所属机器人编码',
    robot_name string COMMENT '所属机器人名称',
    -- tenant_code,
    tenant_id string COMMENT '租户编码',
    -- rcu_code,
    rcu_id string COMMENT 'RCU编码',
    device_id string COMMENT '设备ID',
    robot_type_id int COMMENT '设备类型',
    type_id int COMMENT '事件类型ID',
    type_name string COMMENT '事件类型名称',
    event_time string COMMENT '事件时间',
    level int COMMENT '事件级别',
    details string COMMENT '事件详情',
    ext_data string COMMENT '扩展字段',
    pic_url string COMMENT '事件图片URL',
    location_info string COMMENT '事件发生时机器人位置信息',
    create_time string COMMENT '创建时间',
    read_flag int COMMENT '读取标识：0-未读，1-已读',
    read_flag_name string COMMENT '读取标识：0-未读，1-已读',
    update_time string COMMENT '更新时间',
    remark string COMMENT '备注',
    msg_type_id string COMMENT '消息类型id',
    --1：insert  2:update 3:delete
    db_op int comment 'binlog类型',
    k8s_env_name string comment 'k8s环境名称'
)
partitioned by (dt string comment '分区日期')
stored as parquet;