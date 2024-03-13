-- cdmdwd.dwd_harix_vmd_robot_detect_video_info_i_d 机器人采集的视频信息
create external table cdmdwd.dwd_harix_vmd_robot_detect_video_info_i_d (
    event_id string comment '取guid和video_url的md5值',
    guid string comment 'crss的数据取id值,harix-skill-vision取guid',
    robot_id string comment 'robot id',
    robot_type_inner_name string comment 'robot_type_inner_name',
    tenant_id string comment '租户',
    type string comment '视频类型',
    type_name string comment '视频类型名称',
    video_url string comment '视频地址',
    source string comment '数据来源系统',
    event_time string comment '信息采集时间',
    ext string comment '扩展数据',
    k8s_env_name string comment '数据来源环境'
)
comment '机器人采集的视频信息'
partitioned by(dt string comment 'dt分区字段')
stored as parquet
location '/data/cdmdwd/dwd_harix_vmd_robot_detect_video_info_i_d';