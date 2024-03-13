create external table cdmods.ods_crio_db_c0001_t_device_i_d (
    id bigint comment '设备id,业务库自增id',
    asset_code string comment '资产编码',
    product_type_code string comment '类型编码',
    product_type_code_name string comment '类型编码名称',
    supplier_code string comment '供应商编码',
    supplier_code_name string comment '供应商code',
    product_id bigint comment '产品ID',
    product_id_name string comment '产品名字',
    device_code string comment '设备code',
    device_name string comment '设备名称',
    device_model string comment '设备型号',
    software_version string comment '软件版本',
    hardware_version string comment '硬件版本',
    quality_date string comment '供应商质保期',
    customer_quality_date string comment '客户质保期',
    status bigint comment '1正常 9删除 DEFAULT 1',
    product_date string comment '生产日期',
    tenant_code string comment '租户code',
    create_time string comment '创建时间',
    update_time string comment '修改时间',
    environment string comment '属于哪个环境  test231  prod231',
    sku string comment '设备sku',
    asset_type string comment '资产类型',
    roc_delivery_status bigint  COMMENT 'ROC交付状态 0 已交付 1 未交付 2 交付中 3 回收中',
    is_special bigint  COMMENT '1常规资产 0特殊资产',
    serial_number string COMMENT '序列号',
    operating_status bigint COMMENT '运营状态 1 空闲 2 测试中 3 演示中 4 运营中 5 交付中',
    running_status bigint COMMENT '设备运行状态 1 正常 2 停用 3 故障 4维修中',
    asset_status bigint COMMENT '资产状态 1 在册 2 待测 3 研发测试 4 空闲 5 项目中',
    event_time BIGINT comment '事件发生时间',
    bigdata_method string comment 'db操作类型:INSERT;UPDATE;DELETE',
    k8s_env_name string  comment '环境名称'
)
comment 'boss系统资产管理信息表crss_upms.t_device binlog'
PARTITIONED BY (dt string comment '日期')
STORED AS parquet
location '/data/cdmods/ods_crio_db_c0001_t_device_i_d';