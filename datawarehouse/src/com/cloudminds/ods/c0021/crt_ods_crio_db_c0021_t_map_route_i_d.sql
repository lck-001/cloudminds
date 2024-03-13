CREATE EXTERNAL TABLE cdmods.ods_crio_db_c0021_t_map_route_i_d(
     id bigint COMMENT '主键ID',
     name string COMMENT '名称',
     route_selection string COMMENT '坐标类型,0-横向路径1-纵向路径',
     start_point string COMMENT '起始点',
     area_rect string COMMENT '区域范围',
     route_points string COMMENT '路径坐标串',
     second_route_points string COMMENT '纵向坐标点',
     version int COMMENT '乐观锁版本号',
     code string COMMENT '唯一编码',
     route_id string COMMENT '路径分区ID',
     map_id bigint COMMENT '地图主键ID',
     create_time string COMMENT '创建时间',
     update_time string COMMENT '修改时间',
     robot_id bigint COMMENT '机器人主键ID',
     map_name string COMMENT '所属地图名称',
     map_code string COMMENT '所属地图编码',
     event_time BIGINT comment '事件发生时间',
     bigdata_method string comment 'db操作类型:INSERT;UPDATE;DELETE',
     k8s_env_name string  comment '环境名称'
) COMMENT '清洁-crss_map-t_map_route'
PARTITIONED BY (dt string COMMENT '日期')
STORED AS parquet
location '/data/cdmods/ods_crio_db_c0021_t_map_route_i_d/';