CREATE TABLE cdmods.ods_crio_db_c0001_t_rcu_s_d (
  id int comment '',
  tenant_code string,
  rcu_code string,
  imei string COMMENT 'imei',
  did string COMMENT 'did',
  rcu_name string  COMMENT 'rcu名字',
  model string  COMMENT 'model',
  os string COMMENT '操作系统',
  version_code string,
  description string COMMENT  '描述',
  create_time string  COMMENT '创建时间',
  update_time string  COMMENT '修改时间',
  environment string COMMENT '属于哪个环境  test231  prod231',
  status int  COMMENT '1正常 9删除',
  manufacturer string,
  k8s_env_name string COMMENT '环境名'
) 
PARTITIONED BY(dt string COMMENT '分区日期字段')
STORED AS PARQUET