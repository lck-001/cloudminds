create external table cdmods.ods_crio_db_c0001_t_asset_record_i_d (
    id bigint COMMENT 'id',
    asset_code string COMMENT '资产编码',
    robot_code string COMMENT '设备编码',
    `action` bigint COMMENT '操作类型 1 交付 2 内部领用 3 报障 4 维修 5 回库 6 入库 7 调拨 ',
    tenant_id bigint COMMENT '租户id',
    tenant_code string COMMENT '租户编码',
    customer_code string COMMENT '客户编码',
    order_code string COMMENT '订单编码',
    ext_data string COMMENT '扩展信息',
    staff_id bigint COMMENT '操作人员ID',
    start_time string COMMENT '开始时间',
    end_time string COMMENT '结束时间',
    create_time string COMMENT '创建时间',
    update_time string COMMENT '更新时间',
    status bigint COMMENT '状态 1 空闲 2 测试中 3 演示中 4 运营中 5 交付中 6 待回收 7正常8停用9故障10维修中',
    cause bigint COMMENT '故障原因 1 达闼原因 2 客户原因',
    staff_name string COMMENT '操作员名字',
    event_time BIGINT comment '事件发生时间',
    bigdata_method string comment 'db操作类型:INSERT;UPDATE;DELETE',
    k8s_env_name string  comment '环境名称'
)
comment 'crss_upms.t_asset_record binlog'
PARTITIONED BY (dt string comment '日期')
STORED AS parquet
location '/data/cdmods/ods_crio_db_c0001_t_asset_record_i_d';