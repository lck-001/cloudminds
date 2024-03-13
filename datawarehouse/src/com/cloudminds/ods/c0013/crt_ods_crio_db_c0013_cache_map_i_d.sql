CREATE EXTERNAL TABLE cdmods.ods_crio_db_c0013_cache_map_i_d(
  id int comment "id",
  robotid string comment "机器人id",
  mapjson string comment "地区json",
  mapimage string comment "地区图片",
  poijson string comment "poijson",
  mapid string comment "smartmap_id",
  event_time BIGINT comment '事件发生时间',
  bigdata_method string comment 'db操作类型:INSERT;UPDATE;DELETE',
  k8s_env_name string  comment '环境名称'
) COMMENT 'cache_map'
PARTITIONED BY (dt string COMMENT '日期')
STORED AS parquet
location '/data/cdmods/ods_crio_db_c0013_cache_map_i_d';