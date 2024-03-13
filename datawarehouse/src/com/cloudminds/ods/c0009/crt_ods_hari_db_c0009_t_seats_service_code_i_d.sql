--------------hari t_seats_service_code 表信息---------
CREATE EXTERNAL TABLE cdmods.ods_hari_db_c0009_t_seats_service_code_i_d(
  seat_id bigint COMMENT '坐席id',
  service_code string COMMENT '服务编码',
  event_time bigint comment '事件发生时间',
  bigdata_method string comment 'db操作类型:INSERT;UPDATE;DELETE',
  k8s_env_name string comment '环境名称'
) COMMENT 'hari.t_seats_service_code 表信息,来源Binlog'
PARTITIONED BY ( dt string COMMENT '日期' )
STORED AS parquet
location '/data/cdmods/ods_hari_db_c0009_t_seats_service_code_i_d';