-- cdmods.ods_crio_db_c0001_t_dict_s_d boss系统字典表crss_upms.t_dist
create external table cdmods.ods_crio_db_c0001_t_dict_s_d(
     id int comment '字典唯一id标识',
     name string comment '名称',
     code string comment '编码',
     type int comment '类型 1产品类型  2供应商 3租户所属环境 4租户所在行业 5子系统 6租户类型 7客户性质 8客户来源 9客户类别 10客户所在行业 11信用状态 12联系策略 13购买策略 14客户规模 15行业地位 16结算方式 17客户地区 18资产类型',
     second_code string comment '第二编码',
     create_time string comment '创建时间'
)
comment 'boss系统字典表crss_upms.t_dict:包含产品类型和供应商信息'
PARTITIONED BY (dt string COMMENT '日期')
STORED AS parquet;