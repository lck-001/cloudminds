CREATE EXTERNAL TABLE cdmods.ods_crio_db_c0011_cleanx_task_log_i_d(
     id bigint comment '任务ID',
     tenant_id int comment '租户ID',
     tenant_code string comment '租户编码',
     robot_type string comment '所属机器人类型',
     robot_code string comment '所属机器人编码',
     robot_name string comment '所属机器人名称',
     task_type int comment '所属任务类型',
     work_types string comment '所属工作类型',
     task_code string comment '所属任务编码',
     task_name string comment '所属任务名称',
     area_finish float comment '完成清洁面积(m2)',
     area_plan float comment '计划清洁面接(m2)',
     water float comment '消耗的水量(L)',
     distance float comment '行驶距离(m)',
     `percent` float comment '覆盖率百分比',
     battery float comment '消耗电量百分比',
     efficiency float comment '清洁效率(m2/h)',
     speed float comment '平均速率(m/s)',
     start_time string comment '清洁开始时间',
     end_time string comment '清洁结束时间',
     version int comment '乐观锁版本号',
     status int comment '任务状态，0：未开始 1：执行中 2：暂停 3：结束 9：异常',
     map_code string comment '所属地图编码',
     map_name string comment '所属地图名称',
     create_time string comment '创建时间',
     update_time string comment '更新时间',
     task_id int comment '任务ID,仅供参考，因为任务数据会被删掉',
     cleanx_id int comment '机器人ID',
     start_time_utc string comment '任务开始的UTC时间',
     end_time_utc string comment '任务结束的UTC时间',
     schedule_time string comment '任务计划执行时间',
     task_uuid string comment '任务uuid,随上报的任务状态变更时带上来的',
     record_state int comment '录制状态,0:启动成功 1：启动录制失败 2：停止录制成功 3:停止录制失败 9:不需要录制',
     event_flag int comment '事件生成标识,0:未生成 1:已生成',
     event_time BIGINT comment '事件发生时间',
     bigdata_method string comment 'db操作类型:INSERT;UPDATE;DELETE',
     k8s_env_name string  comment '环境名称'
) COMMENT '清洁-crss_cxms-cleanx_task_log'
PARTITIONED BY (dt string COMMENT '日期')
STORED AS parquet
location '/data/cdmods/ods_crio_db_c0011_cleanx_task_log_i_d/';