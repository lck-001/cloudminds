CREATE EXTERNAL TABLE cdmods.ods_crio_db_c0011_cleanx_task_route_i_d(
     id bigint comment "任务路线ID",
     cleanx_task_id bigint comment "所属任务ID",
     name string comment "名称",
     version bigint comment "乐观锁版本号",
     route_id string comment "路径分区ID",
     map_name string comment "所属地图名称",
     map_code string comment "所属地图编码",
     event_time BIGINT comment '事件发生时间',
     bigdata_method string comment 'db操作类型:INSERT;UPDATE;DELETE',
     k8s_env_name string  comment '环境名称'
) COMMENT '清洁-crss_cxms-cleanx_task_route'
PARTITIONED BY (dt string COMMENT '日期')
STORED AS parquet;