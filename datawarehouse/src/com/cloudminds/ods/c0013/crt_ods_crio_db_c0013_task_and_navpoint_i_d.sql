CREATE EXTERNAL TABLE cdmods.ods_crio_db_c0013_task_and_navpoint_i_d(
    taskid int comment 'taskid',
    navpointid int comment 'navpointid',
    event_time BIGINT comment '事件发生时间',
    bigdata_method string comment 'db操作类型:INSERT;UPDATE;DELETE',
    k8s_env_name string  comment '环境名称'
) COMMENT 'task_and_navpoint'
PARTITIONED BY (dt string COMMENT '日期')
STORED AS parquet location '/data/cdmods/ods_crio_db_c0013_task_and_navpoint_i_d';