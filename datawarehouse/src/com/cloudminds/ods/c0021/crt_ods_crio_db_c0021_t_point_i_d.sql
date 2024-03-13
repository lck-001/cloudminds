CREATE EXTERNAL TABLE cdmods.ods_crio_db_c0021_t_point_i_d(
     id bigint COMMENT 'id',
     name string COMMENT '兴趣点名称',
     map_id bigint COMMENT '地图ID',
     x string COMMENT '宽',
     y string COMMENT '高',
     z string COMMENT '',
     yaw string COMMENT '',
     pitch bigint COMMENT '',
     roll bigint COMMENT '',
     type bigint COMMENT '',
     version bigint  COMMENT '乐观锁版本号',
     phone string COMMENT '电话',
     `sort` int COMMENT '序号',
     classification bigint COMMENT '分类',
     event_time BIGINT comment '事件发生时间',
     bigdata_method string comment 'db操作类型:INSERT;UPDATE;DELETE',
     k8s_env_name string  comment '环境名称'
) COMMENT '清洁-crss_map-t_point'
PARTITIONED BY (dt string COMMENT '日期')
STORED AS parquet
location '/data/cdmods/ods_crio_db_c0021_t_point_i_d';