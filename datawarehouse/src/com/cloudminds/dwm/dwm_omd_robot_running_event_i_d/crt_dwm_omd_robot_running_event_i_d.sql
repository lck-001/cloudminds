--  modify by dave.liang at 2021-09-22   由于 cross 的该表日志延迟很大 超多30天 ，且 不准，暂时废弃！！！！
create external table cdmdwm.dwm_omd_robot_running_event_i_d
(

    service_code string comment '机器人和客服服务码',
    switch_id string comment '',
    switch_data string comment '',
    err_msg string comment '错误信息',
    err_code int comment '错误编码',
    rcu_id string comment 'rcu id',
    robot_account_id string comment '机器人账户，roc系统中创建的机器人账号',
    switch_version string comment '版本号',
    sid string comment '会话id',
    event_type string comment '事件类型:robotOnlineTick在线（十分钟持续在线发一次）robotConnect上线 robotAbnormalDisconnect异常下线',
    event_time string comment '事件发生时时间',
    k8s_env_name string comment 'k8s环境名称',
    ext string comment '保留的原始数据',
    sv_agent_id             string comment 'NLP库的AgentID',
--           tenant dim
    tenant__tenant_id string comment '租户id',
    tenant__tenant_name string comment '租户名称',
    tenant__tenant_type string comment '租户类型',
    tenant__industry_id string comment '租户所属行业id',
    tenant__sub_sys string comment '子系统',
    tenant__time_zone string comment '时区',
    tenant__net_env_id string comment '网络环境',
    tenant__mcs_area string comment 'mcs区域',
    tenant__device_num int comment '最大设备数',
    tenant__region string comment '地区',
    tenant__address string comment '地址',
    tenant__contact string comment '租户联系人',
    tenant__phone string comment '联系电话',
    tenant__telephone string comment '联系电话',
    tenant__seller string comment '销售人员',
    tenant__email string comment '邮箱地址',
    tenant__status int comment '租户状态: -1-删除;0-正常;1-待开通;2-锁定',
    tenant__status_name string comment '租户状态',
    tenant__charge_time string comment '计费开始时间',
    tenant__expired_time string comment '租户过期时间',
    tenant__logo string comment '租户logo地址',
    tenant__description string comment '租户简要描述',
    tenant__brand string comment '客户品牌化参数',
    tenant__is_need_vpn int comment '是否需要VPN: 0-不需要,1-需要',
    tenant__create_time string comment '租户创建时间',
    tenant__industry_name string comment  '租户行业名',
--          robot dim
    robot__robot_id string comment '机器人id标识，既是robot_code',
    robot__robot_name string comment '机器人名称',
    robot__robot_type_id string comment '机器人类型',
    robot__robot_type_inner_name string comment '机器人类型名(研发)',
    robot__robot_type_name string comment '机器人类型名',
    robot__asset_code string comment '资产code',
    robot__asset_type string comment '资产类型',
    robot__product_type_code string comment '产品类型code',
    robot__product_type_code_name string comment '产品类型code所对应的名称',
    robot__product_id string comment '产品ID',
    robot__product_id_name string comment '产品名字',
    robot__assets_status int comment '资产状态 0-在册 1-在库 2-出库',
    robot__assets_status_name string comment '资产状态 在册 在库 出库',
    robot__status int comment '机器人状态 robot状态: -1-删除 0-正常 1-停用',
    robot__status_name string comment '机器人状态',
    robot__robot_manufacturer_id string comment '机器人制造商id',
    robot__robot_manufacturer_name string comment '机器人制造商名称',
    robot__model string comment 'robot型号,roc中model boss中device_model',
    robot__os string comment '操作系统平台',
    robot__version_code string comment '版本号',
    robot__software_version string comment '机器人软件版本',
    robot__hardware_version string comment '机器人硬件版本',
    robot__head_url string comment '展示头像',
    robot__regist_time string comment '注册时间',
    robot__bind_status int comment '机器人受控状态: -1-删除 0-注册未激活 1-激活可用 2-通知VPN注册',
    robot__bind_status_name string comment '机器人受控状态名',
    robot__is_hi_service int comment '人工服务: 0-禁止，1-允许',
    robot__is_privacy_enhance int comment '隐私增强：0-关闭，1-开启',
    robot__remind_policy_id string comment '提醒策略id',
    robot__sku string comment '机器人SKU',
    robot__quality_date string comment '供应商质保期',
    robot__customer_quality_date string comment '客户质保期',
    robot__out_type_state int comment '出库类型 0 交付出库 1 维修出库 2 报废出库 3 交付给客户 4 交付给测试',
    robot__out_type_state_name string comment '出库类型 0 交付出库 1 维修出库 2 报废出库 3 交付给客户 4 交付给测试',
    robot__product_date string comment '生产日期',
    robot__in_stock_date string comment '入库日期',
    robot__out_stock_date string comment '出库日期',
    robot__in_stock_staff_id string comment '入库操作人员ID',
    robot__in_stock_staff_name string comment '入库操作人员姓名',
    robot__out_stock_staff_id string comment '出库操作人员ID',
    robot__out_stock_staff_name string comment '出库操作人员姓名',
    robot__robot_note string comment '备注',
    robot__description string comment '机器人信息描述',
    robot__create_time string comment '机器人创建时间',
    robot__operating_status int comment '机器人运营状态',
    robot__operating_status_name string comment '机器人运营状态名称',

--            customer dim
    customer__customer_id string comment '客户id',
    customer__customer_name string comment '客户名称',
    customer__pid string comment '客户父级',
    customer__level string comment '当前客户层级',
    customer__region string comment '客户所在区域',
    customer__address string comment '客户地址',
    customer__website string comment '客户网址',
    customer__nature_code string comment '客户性质编码',
    customer__nature_name string comment '客户性质名称',
    customer__credit_code string comment '客户信用编码',
    customer__credit_name string comment '客户信用名称',
    customer__source_code string comment '客户来源编码',
    customer__source_name string comment '客户来源名称',
    customer__scale_code string comment '公司规模编码',
    customer__scale_name string comment '公司规模名称',
    customer__status_code string comment '客户行业地位编码',
    customer__status_name string comment '客户行业地位名称',
    customer__contact_code string comment '联系策略编码',
    customer__contact_name string comment '联系策略名称',
    customer__purchase_code string comment '客户购买策略编码',
    customer__purchase_name string comment '客户购买策略名称',
    customer__employment_code string comment '客户从业时间编号',
    customer__employment_name string comment '客户从业时间名称',
    customer__settlement_code string comment '结算方式编码',
    customer__settlement_name string comment '结算方式名称',
    customer__employer_num int comment '客户员工数量',
    customer__category_code string comment '客户类别编码',
    customer__category_name string comment '客户类别名称',
    customer__create_time string comment '客户创建时间'
)
    comment '机器人在线事件表'
    partitioned by(dt string comment 'dt分区字段')
    stored as parquet ;
