create external table cdmdwd.dwd_crio_pmd_asset_record_a_d (
    asset_record_id string COMMENT 'id',
    asset_code string COMMENT '资产编码',
    robot_id string COMMENT '设备编码',
    action_type int COMMENT '操作类型 1 交付 2 内部领用 3 报障 4 维修 5 回库 6 入库 7 调拨 ',
    action_type_name string COMMENT '操作类型 1 交付 2 内部领用 3 报障 4 维修 5 回库 6 入库 7 调拨 ',
    tenant_id string COMMENT '租户id',
    customer_id string COMMENT '客户编码',
    order_id string COMMENT '订单编码',
    ext_data string COMMENT '扩展信息',
    record_start_time string COMMENT '开始时间',
    record_end_time string COMMENT '结束时间',
    status int COMMENT '状态 1 空闲 2 测试中 3 演示中 4 运营中 5 交付中 6 待回收 7 正常 8 停用 9 故障 10 维修中',
    status_name string COMMENT '状态 1 空闲 2 测试中 3 演示中 4 运营中 5 交付中 6 待回收 7 正常 8 停用 9 故障 10 维修中',
    cause_type int COMMENT '故障原因 1 达闼原因 2 客户原因',
    cause_type_name string COMMENT '故障原因 1 达闼原因 2 客户原因',
    staff_id string COMMENT '操作人员ID',
    staff_name string COMMENT '操作员名字',
    event_time string comment '事件发生时间,选的update_time作为事件时间',
    k8s_env_name string  comment '环境名称',
    create_time string COMMENT '创建时间',
    update_time string COMMENT '更新时间'
)
comment 'crss_upms.t_asset_record binlog'
STORED AS parquet
location '/data/cdmdwd/dwd_crio_pmd_asset_record_a_d';