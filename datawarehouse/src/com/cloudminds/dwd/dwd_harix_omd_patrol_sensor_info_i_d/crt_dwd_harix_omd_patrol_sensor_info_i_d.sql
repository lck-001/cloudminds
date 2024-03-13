-- cdmdwd.dwd_harix_omd_patrol_sensor_info_i_d 安保机器人传感器采集的信息
create external table cdmdwd.dwd_harix_omd_patrol_sensor_info_i_d (
    event_id string comment '使用guid进行填充',
    robot_id string comment 'robot id',
    rcu_id string comment 'rcu id',
    robot_account_id string comment 'robot account id',
    tenant_id string comment '租户',
    event_time string comment '信息采集时间',
    source string comment '数据来源系统',
    map_id string comment '地图id',
    map_name string comment '地图名称',
    big_light_status int comment '大灯状态 0false 1true',
    whistle_warning_status int comment '鸣警状态 0false 1true',
    charging_pile_status int comment '充电桩状态 0false 1true',
    charging_online_status int comment '插线充电状态 0false 1true',
    battery_charging_status int comment '充电状态 0没充电 1充电',
    navi_target_speed int comment '目标速度',
    navi_patrolling_route_name string comment '安保路线名',
    navi_emergency_stop_switch_status int comment '紧急停止开关状态 0关闭 1开启',
    navi_warning_light_status int comment '警告灯状态 0关闭 1开启',
    navi_speed int comment '速度',
    current_battery int comment '当前电量',
    atmosphere string comment '气压',
    pm25 string comment 'pm25',
    co string comment '一氧化碳',
    co2 string comment '二氧化碳',
    temperature string comment '温度',
    humidity string comment '湿度',
    fog string comment '烟雾',
    tvoc string comment '有机气体',
    top_camera_connection_state int comment '顶端摄像头连接状态 0关闭 1开启',
    k8s_env_name string comment '数据来源环境'
)
comment '安保机器人传感器采集的信息'
partitioned by(dt string comment 'dt分区字段')
stored as parquet
location '/data/cdmdwd/dwd_harix_omd_patrol_sensor_info_i_d';