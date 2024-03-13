----------------------------------hari t_seats表信息------------------------
CREATE EXTERNAL TABLE cdmods.ods_hari_db_c0009_t_seats_s_d(
  id bigint COMMENT '坐席id',
  tenant_id bigint COMMENT '隶属租户id',
  tenant_code string COMMENT '隶属租户code',
  login_id string COMMENT '用户登录id',
  pwd string COMMENT '登录密码',
  user_name string COMMENT '姓名',
  email string COMMENT '邮件地址',
  phone_code string COMMENT '手机号码',
  type tinyint COMMENT '用户来源: 0-手工录入;1-文件导入;2-ad导入;3-ad同步',
  status tinyint COMMENT '坐席状态: -1-删除;0-停用;1-启用;2-无效',
  create_time timestamp COMMENT '创建时间',
  update_time timestamp COMMENT '更新时间',
  operator_id bigint COMMENT '操作员id',
  seats_group_id bigint COMMENT '坐席分组组id',
  sync_batch_no bigint COMMENT '同步批次号',
  k8s_env_name string COMMENT '环境名称'
) COMMENT 'hari.t_seats prod-251表信息'
PARTITIONED BY (dt string COMMENT '日期')
STORED AS parquet;