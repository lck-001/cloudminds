CREATE EXTERNAL TABLE  cdmdim.dim_cmd_domain_a_d(
  domain_id string COMMENT 'domain 主键id',
  domain_name string COMMENT 'domain 名称',
  domain_info string COMMENT 'domain 信息',
  domain_type string COMMENT 'domain类型 乱码 废弃字段',
  status string COMMENT '状态,乱码 废弃字段',
  url string COMMENT 'URL',
  call_service string COMMENT '调用接口',
  is_release int COMMENT '是否发布 1-发布,0-未发布',
  is_closed_recg int COMMENT '关闭后是否意图识别, 1-关闭后能意图识别,0-关闭后不能意图识别',
  closed_hit string COMMENT '关闭后命中话术，多句回复用&&隔开',
  keyword int COMMENT '1：代表走关键词服务，此时服务在redis中算是停用状态 其他：不走服务 ',
  domain_switch_hit string COMMENT '??????',
  k8s_env_name string COMMENT '环境名',
  create_time string COMMENT 'domain创建时间',
  update_time string COMMENT 'domain更新时间'
)
COMMENT 'domain维度表'
STORED AS PARQUET
location '/data/cdmdim/dim_cmd_domain_a_d';