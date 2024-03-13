CREATE EXTERNAL TABLE cdmods.ods_hari_db_c0004_t_login_seats_i_d(
  seat_id bigint COMMENT '坐席的id',
  login_id string COMMENT '用户登录id',
  seats_group_id bigint COMMENT '坐席组的id',
  portal_status string COMMENT '来自portal的状态：online,offline,busy',
  connection_status int COMMENT '连接的状态: 0:未连接，1:连接',
  queue_server string COMMENT '登陆的排队服务器',
  service_count int COMMENT '正在服务的人数',
  ability_codes string COMMENT '技能组',
  tenant_codes string COMMENT '租户组',
  create_time string COMMENT '创建时间',
  update_time string COMMENT '更新时间',
  env_id string COMMENT '环境Id',
  event_time BIGINT comment '事件发生时间',
  bigdata_method string comment 'db操作类型:INSERT;UPDATE;DELETE',
  k8s_env_name string  comment '环境名称'
) COMMENT 'hari.t_login_seats 表信息,坐席在线表'
PARTITIONED BY (dt string COMMENT '日期')
STORED AS parquet
location '/data/cdmods/ods_hari_db_c0004_t_login_seats_i_d';