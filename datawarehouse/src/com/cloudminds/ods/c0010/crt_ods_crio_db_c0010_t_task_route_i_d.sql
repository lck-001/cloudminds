create external table cdmods.ods_crio_db_c0010_t_task_route_i_d(
     id bigint comment "任务路线ID",
     task_id bigint comment "所属任务ID",
     name string comment "名称",
     map_route_id bigint comment "地图路径ID",
     map_route_name string comment "地图路径名称",
     route_id string comment "地图路径内部id",
     map_id bigint comment "地图id",
     map_name string comment "所属地图名称",
     map_code string comment "所属地图编码",
     create_time string comment "创建时间",
     version int comment "乐观锁版本号",
     event_time BIGINT comment '事件发生时间',
     bigdata_method string comment 'db操作类型:INSERT;UPDATE;DELETE',
     k8s_env_name string  comment '环境名称'
)
comment 'crss_task.t_task_route'
PARTITIONED BY (dt string COMMENT '日期')
STORED AS parquet
location '/data/cdmods/ods_crio_db_c0010_t_task_route_i_d';