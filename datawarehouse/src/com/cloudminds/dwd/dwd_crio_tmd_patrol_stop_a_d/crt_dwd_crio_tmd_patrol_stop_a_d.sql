-- cdmdwd.dwd_crio_tmd_patrol_stop_a_d 巡逻机器人任务stop事件
create external table cdmdwd.dwd_crio_tmd_patrol_stop_a_d (
    oid string comment 'mongo id',
    event_id string comment '没有uuid之前使用mongo id填充,有uuid使用uuid进行填充',
    robot_id string comment 'robot id',
    tenant_id string comment '租户',
    project_code string comment '项目code',
    project_name string comment '项目名称',
    event_type string comment '巡逻事件类型,巡逻事件id,数仓的type',
    event_type_name string comment '巡逻事件类型,巡逻事件名称,数仓的type',
    description string comment '任务描述,当前状态执行状态的一个描述',
    programme_id string comment '节目单id,对应robot_task中的id',
    programme_name string comment '节目单名称',
    task_exec_start_time string comment '本次任务开始时间,存在数据没有年月日,研发未填写和循环任务这两种情况下,没有年月日,通过入库时间填充对应的年月日',
    task_exec_stop_time string comment '本次任务停止时间,存在数据没有年月日,研发未填写和循环任务这两种情况下,没有年月日,通过入库时间填充对应的年月日',
    running_time int comment '本次任务运行时长,单位s',
    finish_times int comment '本次任务完成圈数',
    mileage decimal(18,2) comment '本次任务完成里程，单位米',
    finish_point_tasks int comment '本次任务完成巡更点数量',
    current_battery int comment '当前电量',
    driven_mileage decimal(18,2) comment '机器总里程，单位米',
    avoidance_times int comment '本次任务避障次数',
    tts_play_times int comment '本次任务语音播报次数',
    average_mileage decimal(18,2) comment '本次任务平均每圈里程',
    average_battery int comment '本次任务平均每圈耗电',
    plan_repeat_times int comment '本次任务计划圈数',
    plan_point_tasks int comment '本次任务计划巡更点数量',
    pub_time string comment '记录入库时间',
    event_time string comment '巡逻事件时间,这里用的入库时间进行填充的',
    k8s_env_name string comment '数据来源环境'
)
comment 'psp机器人每次巡逻任务中，任务状态的改变触发的一次巡逻任务事件'
stored as parquet
location '/data/cdmdwd/dwd_crio_tmd_patrol_stop_a_d';