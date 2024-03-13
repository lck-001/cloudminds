-- cdmods.ods_crio_db_c0001_t_customer_s_d boss系统客户信息
create external table cdmods.ods_crio_db_c0001_t_customer_s_d(
     id int comment '客户id标识',
     uuid string comment '名称',
     name string comment '客户id,使用客户code码',
     salesman string comment '客户名称',
     father_id string comment '客户父级',
     region_code string comment '客户所在区域',
     region_name string comment '客户地址',
     nature_code string comment '客户性质编码',
     nature_name string comment '客户性质名称',
     source_code string comment '客户来源编码',
     source_name string comment '客户来源名称',
     category_code string comment '客户类别编码',
     category_name string comment '客户类别名称',
     industry_code string comment '客户所属行业编码',
     industry_name string comment '客户所属行业编码',
     credit_code string comment '客户信用编码',
     credit_name string comment '客户信用名称',
     contact_code string comment '联系策略编码',
     contact_name string comment '联系策略名称',
     purchase_code string comment '客户购买策略编码',
     purchase_name string comment '客户购买策略名称',
     staff_num int comment '客户员工数量',
     scale_code string comment '公司规模编码',
     scale_name string comment '公司规模名称',
     status_code string comment '客户行业地位编码',
     status_name string comment '客户行业地位名称',
     employment_code string comment '客户从业时间编号',
     employment_name string comment '客户从业时间名称',
     settlement_code string comment '结算方式编码',
     settlement_name string comment '结算方式名称',
     agent string comment '希望代理',
     phone string comment '电话',
     fax string comment '传真',
     mail string comment '邮箱',
     website string comment '网址',
     address string comment '通讯地址',
     post_code string comment '邮编',
     consumption_num int comment '交易次数',
     money decimal(10,2) comment '首次交易金额',
     first_deal_time string comment '首次交易时间',
     lately_deal_time string comment '最近交易时间',
     create_time string comment '创建时间',
     update_time string comment '更新时间'
)
comment 'boss系统客户信息表crss_upms.t_customer'
PARTITIONED BY (dt string COMMENT '日期')
STORED AS parquet;