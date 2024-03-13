create external table cdmdim.dim_tmd_task_sh_d (
    task_id string comment '任务id',
    task_name string comment '任务名称',
    robot_id string comment '机器人id',
    map_id string comment '地图id',
    map_name string comment '地图名称',
    task_type_id int comment '任务类型id',
    task_type_name string comment '任务类型名称',
    work_types string comment '工作类型',
    work_type_names string comment '工作类型名称',
    schedule_type string comment '调度类型：fixed,loop',
    schedule_start_time string comment '调度开始时间',
    schedule_end_time string comment '调度结束时间',
    schedule_days string comment '调度日期',
    repeat_times int comment '循环次数',
    repeat_type int comment '循环周期，日循环：1，周循环：7，月循环：30',
    repeat_type_name string comment '循环周期名称',
    sub_task string comment '子任务',
    is_del int comment '1删除 0没删除',
    create_time string comment '创建时间',
    update_time string comment '更新时间',
    start_time string comment '有效开始时间',
    end_time string comment '有效结束时间',
    k8s_env_name string comment '环境'
    )
comment '机器人任务维表'
partitioned by (dt string comment '分区日期')
stored as parquet;