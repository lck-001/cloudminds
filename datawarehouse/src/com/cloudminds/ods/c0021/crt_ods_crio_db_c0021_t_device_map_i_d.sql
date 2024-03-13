CREATE EXTERNAL TABLE cdmods.ods_crio_db_c0021_t_device_map_i_d(
     id bigint COMMENT 'ID',
     device_id bigint COMMENT '机器人ID',
     map_id bigint COMMENT '地图ID',
     create_time string COMMENT '创建时间',
     version int COMMENT '乐观锁版本号',
     update_time string COMMENT '更新时间' ,
     device_name string COMMENT '设备名字冗余',
     device_code string COMMENT '设备编码冗余',
     map_name string COMMENT '地图名字冗余',
     tenant_id int COMMENT '租户ID',
     tenant_code string COMMENT '租户编码',
     current_map_version int COMMENT '机器人当前地图版本号',
     event_time BIGINT comment '事件发生时间',
     bigdata_method string comment 'db操作类型:INSERT;UPDATE;DELETE',
     k8s_env_name string  comment '环境名称'
) COMMENT '清洁-crss_map-t_device_map'
PARTITIONED BY (dt string COMMENT '日期')
STORED AS parquet
location '/data/cdmods/ods_crio_db_c0021_t_device_map_i_d';