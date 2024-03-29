-- cdmdwd.dwd_crio_tmd_cleanx_task_log_i_d 清洁机器人日志
create external table cdmdwd.dwd_crio_tmd_cleanx_task_log_a_d (
     log_id string comment '日志id',
     tenant_id string comment '租户id',
     robot_type string comment '机器人类型',
     robot_id string comment '机器人id',
     robot_name string comment '机器人名称',
     schedule_type string comment '调度类型：fixed,loop',
     work_types string comment '工作类型',
     work_type_names string comment '工作类型名称',
     task_code string comment '所属任务编码',
     task_name string comment '所属任务名称',
     area_finish decimal(8,2) comment '完成清洁面积(m2)',
     area_plan decimal(8,2) comment '计划清洁面接(m2)',
     water decimal(8,3) comment '消耗的水量(L)',
     distance decimal(8,2) comment '行驶距离(m)',
     `percent` decimal(5,2) comment '覆盖率百分比',
     battery decimal(5,2) comment '消耗电量百分比',
     efficiency decimal(8,2) comment '清洁效率(m2/h)',
     speed decimal(8,2) comment '平均速率(m/s)',
     start_time string comment '清洁开始时间',
     end_time string comment '清洁结束时间',
     version int comment '乐观锁版本号',
     status int comment '任务状态',
     status_name string comment '任务状态名称',
     map_code string comment '所属地图编码',
     map_name string comment '所属地图名称',
     create_time string comment '创建时间',
     update_time string comment '更新时间',
     task_id string comment '任务ID,仅供参考',
     cleanx_id string comment '机器人ID',
     schedule_start_time string comment '任务计划执行时间',
     task_uuid string comment '任务uuid,随上报的任务状态变更时带上来的',
     record_state int comment '录制状态',
     record_state_name string comment '录制状态名称',
     event_flag int comment '事件生成标识',
     event_flag_name string comment '事件生成名称',
     event_time string COMMENT '事件时间',
     k8s_env_name string comment '数据来源环境'
)
comment '清洁机器人日志'
stored as parquet
location '/data/cdmdwd/dwd_crio_tmd_cleanx_task_log_a_d';