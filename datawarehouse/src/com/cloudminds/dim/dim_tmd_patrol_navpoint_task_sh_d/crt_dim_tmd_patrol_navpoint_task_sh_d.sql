create external table cdmdim.dim_tmd_patrol_navpoint_task_sh_d (
    navpoint_id string comment '导航点id',
    navpoint_name string comment '导航点名称',
    robot_id string comment '机器人id',
    path_id int comment '路径任务ID',
    path_name string comment '路径名称',
    map_name string comment '地图名称',
    wait_time int comment '停留时间',
    pos_x int comment '位置X',
    pos_y int comment '位置Y',
    fn string comment '导航点任务',
    description string comment '备注',
    k8s_env_name string comment '环境',
    is_del int comment '1删除 0没删除',
    start_time string comment '有效开始时间',
    end_time string comment '有效结束时间')
comment '安保机器人导航点任务'
partitioned by (dt string comment '分区日期')
stored as parquet;