-- cdmods.ods_psp_db_c0013_robot_task_s_d 安保机器人任务
create external table cdmods.ods_psp_db_c0013_robot_task_s_d(
     id int comment '自增主键',
     name string comment '任务名称',
     starttime string comment '任务开始时间',
     endtime string comment '任务结束时间',
     times int comment '任务次数',
     pathid int comment '路径ID',
     taskpath string comment '路径名称',
     robotid string comment '机器人id',
     pubtime string comment '添加时间',
     isdel string comment '是否被删除：yes,no',
     type string comment '任务类型:loop,fixed',
     mapid int comment '地图id',
     mapname string comment '地图名称',
     status string comment '任务状态默认为flase 未开始，true执行中',
     taskstatus string comment '未执行为空，执行过为changed',
     taskstatusfan string comment '任务执行状态默认为 未开始，执行中,暂停,结束',
     k8s_env_name string comment '数据来源环境'
)
comment '货柜订单日志gdb.robot_task'
PARTITIONED BY (dt string COMMENT '日期')
STORED AS parquet;