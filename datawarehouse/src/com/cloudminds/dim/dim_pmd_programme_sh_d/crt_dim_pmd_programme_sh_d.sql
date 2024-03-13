CREATE EXTERNAL TABLE cdmdim.dim_pmd_programme_sh_d (
    programme_id string comment 'programme id 即robot_task中task id',
    programme_name string COMMENT 'programme名字',
    programme_type string COMMENT '节目单任务模式,固定(fixed)还是循环(loop)',
    robot_id string comment '机器人id',
    task_start_time string comment 'programme 定义的task开始时间',
    task_end_time string COMMENT 'programme 定义的task结束时间',
    times int COMMENT '节目单执行次数',
    path_id string COMMENT '当前节目单对应机器人的路径id',
    task_path string comment '任务路径',
    map_id string comment '地图id',
    map_name string COMMENT '地图名称',
    status int comment '节目单对应的任务状态,默认为0',
    status_name String comment '任务状态名称',
    is_del int comment '是否删除,-1:删除;0:正常',
    is_changed int COMMENT '执行过为1,未执行过为0,即业务表中的 执行过changed，未执行为空',
    roc_task_status string COMMENT 'roc返回任务执行状态默认为空 ，未开始,执行中,暂停,结束',
    pub_time string comment 'programme入库时间,修改值后会变化',
    start_time string COMMENT '拉链信息开始时间',
    end_time string COMMENT '拉链信息结束时间',
    k8s_env_name string COMMENT '环境名',
    create_time STRING COMMENT '机器人节目单创建时间',
    update_time STRING COMMENT '机器人节目单更新时间'
)
COMMENT '机器人节目单表,对应robot_task表'
PARTITIONED BY (dt string COMMENT '日期')
STORED AS PARQUET
location '/data/cdmdim/dim_pmd_programme_sh_d';