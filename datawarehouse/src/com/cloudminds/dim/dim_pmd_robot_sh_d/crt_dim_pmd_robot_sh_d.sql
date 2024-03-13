-- cdmdim.dim_pmd_robot_sh_d 机器人维度历史拉链快照表
create external table cdmdim.dim_pmd_robot_sh_d (
    robot_id string comment '机器人id标识，既是robot_code',
    robot_name string comment '机器人名称',
    robot_type_id string comment '机器人类型',
    robot_type_inner_name string comment '机器人类型名(研发)',
    robot_type_name string comment '机器人类型名',
    asset_code string comment '资产code',
    asset_type string comment '资产类型',
    product_type_code string comment '产品类型code',
    product_type_code_name string comment '产品类型code所对应的名称',
    product_id string comment '产品ID',
    product_id_name string comment '产品名字',
    assets_status int comment '资产状态 0-在册 1-在库 2-出库',
    assets_status_name string comment '资产状态 在册 在库 出库',
    status int comment '机器人状态 robot状态: -1-删除 0-正常 1-停用',
    status_name string comment '机器人状态',
    robot_manufacturer_id string comment '机器人制造商id',
    robot_manufacturer_name string comment '机器人制造商名称',
    model string comment 'robot型号,roc中model boss中device_model',
    os string comment '操作系统平台',
    version_code string comment '版本号',
    software_version string comment '机器人软件版本',
    hardware_version string comment '机器人硬件版本',
    head_url string comment '展示头像',
    regist_time string comment '注册时间',
    bind_status int comment '机器人受控状态: -1-删除 0-注册未激活 1-激活可用 2-通知VPN注册',
    bind_status_name string comment '机器人受控状态名',
    is_hi_service int comment '人工服务: 0-禁止，1-允许',
    is_privacy_enhance int comment '隐私增强：0-关闭，1-开启',
    remind_policy_id string comment '提醒策略id',
    sku string comment '机器人SKU',
    quality_date string comment '供应商质保期',
    customer_quality_date string comment '客户质保期',
    out_type_state int comment '出库类型 0 交付出库 1 维修出库 2 报废出库 3 交付给客户 4 交付给测试',
    out_type_state_name string comment '出库类型 0 交付出库 1 维修出库 2 报废出库 3 交付给客户 4 交付给测试',
    product_date string comment '生产日期',
    in_stock_date string comment '入库日期',
    out_stock_date string comment '出库日期',
    in_stock_staff_id string comment '入库操作人员ID',
    in_stock_staff_name string comment '入库操作人员姓名',
    out_stock_staff_id string comment '出库操作人员ID',
    out_stock_staff_name string comment '出库操作人员姓名',
    note string comment '备注',
    description string comment '机器人信息描述',
    start_time string comment '当前机器人记录生效时间',
    end_time string comment '当前机器人记录失效时间',
    k8s_env_name string comment '环境名称',
    create_time string comment '机器人信息创建时间',
    update_time string comment '机器人信息更新时间'
)
comment '机器人维度历史拉链快照表'
partitioned by(dt string comment 'dt分区字段')
stored as parquet
location '/data/cdmdim/dim_pmd_robot_sh_d';



-- cdmdim.dim_pmd_robot_sh_d 机器人维度历史拉链快照表
create external table cdmdim.dim_pmd_robot_sh_d (
    robot_id string comment '机器人id标识，既是robot_code',
    robot_name string comment '机器人名称',
    robot_type_id string comment '机器人类型',
    robot_type_inner_name string comment '机器人类型名(研发)',
    robot_type_name string comment '机器人类型名',
    tenant_id string comment '租户id',
    rcu_id string comment 'rcu id',
    robot_account_id string comment '机器人账户',
    robot_account_name string comment '机器人账户',
    asset_code string comment '资产code',
    asset_type string comment '资产类型 001 达闼固资 002 达闼存货 003 客户资产,默认001',
    asset_type_name string comment '资产类型名称 001 达闼固资 002 达闼存货 003 客户资产,默认001',
    product_type_code string comment '产品类型code',
    product_type_code_name string comment '产品类型code所对应的名称',
    product_id string comment '产品ID',
    product_id_name string comment '产品名字',
    asset_status int comment '资产状态 1 在册 2 待测 3 研发测试 4 空闲 5 项目中',
    asset_status_name string comment '资产状态名称 1 在册 2 待测 3 研发测试 4 空闲 5 项目中',
    status int comment '机器人状态 robot状态: 1-正常 9-删除',
    status_name string comment '机器人状态 robot状态: 1-正常 9-删除',
    roc_status int comment 'roc绑定信息的状态 -1-删除 0-正常 1-停用 2-待删除',
    roc_status_name string comment 'roc绑定信息的状态 -1-删除 0-正常 1-停用 2-待删除',
    robot_manufacturer_id string comment '机器人制造商id',
    robot_manufacturer_name string comment '机器人制造商名称',
    model string comment 'robot型号,roc中model boss中device_model',
    os string comment '操作系统平台',
    version_code string comment '版本号',
    software_version string comment '机器人软件版本',
    hardware_version string comment '机器人硬件版本',
    head_url string comment '展示头像',
    regist_time string comment '注册时间',
    bind_status int comment '机器人受控状态: -1-删除 0-注册未激活 1-激活可用 2-通知VPN注册',
    bind_status_name string comment '机器人受控状态名',
    is_hi_service int comment '人工服务: 0-禁止，1-允许',
    is_privacy_enhance int comment '隐私增强：0-关闭，1-开启',
    remind_policy_id string comment '提醒策略id',
    sku string comment '机器人SKU',
    quality_date string comment '供应商质保期',
    customer_quality_date string comment '客户质保期',
    product_date string comment '生产日期',
    roc_delivery_status int comment 'ROC交付状态 0 已交付 1 未交付 2 交付中 3 回收中,默认为1',
    roc_delivery_status_name string comment 'ROC交付状态名称 0 已交付 1 未交付 2 交付中 3 回收中,默认为1',
    is_special_asset int comment '1常规资产 0特殊资产,默认1',
    serial_number string comment '机器人序列号',
    operating_status int comment '运营状态 1 空闲 2 测试中 3 演示中 4 运营中 5 交付中,默认1',
    operating_status_name string comment '运营状态名称 1 空闲 2 测试中 3 演示中 4 运营中 5 交付中,默认1',
    running_status int comment '运行状态 1 良好 2 故障 3 维修中,默认1',
    running_status_name string comment '运行状态名称 1 良好 2 故障 3 维修中,默认1',
    environment string comment '机器人属于哪个环境',
    description string comment '机器人信息描述',
    start_time string comment '当前机器人记录生效时间',
    end_time string comment '当前机器人记录失效时间',
    k8s_env_name string comment '环境名称',
    create_time string comment '机器人信息创建时间',
    update_time string comment '机器人信息更新时间'
)
comment '机器人维度历史拉链快照表'
partitioned by(dt string comment 'dt分区字段')
stored as parquet
location '/data/cdmdim/dim_pmd_robot_sh_d';