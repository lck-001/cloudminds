-- cdmdwd.dwd_crio_tmd_patrol_obstacle_a_d 巡逻机器人任务避障事件
create external table cdmdwd.dwd_crio_tmd_patrol_obstacle_a_d (
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
    avoidance_time string comment '避障时间点',
    current_location_x int comment '当前点位',
    current_location_y int comment '当前点位',
    current_battery int comment '当前电量',
    pub_time string comment '记录入库时间',
    event_time string comment '巡逻事件时间,这里用的入库时间进行填充的',
    k8s_env_name string comment '数据来源环境'
)
comment 'psp机器人每次巡逻任务中，任务状态的改变触发的一次巡逻任务事件'
stored as parquet
location '/data/cdmdwd/dwd_crio_tmd_patrol_obstacle_a_d';