create external table cdmods.ods_crio_db_c0033_t_task_node_log_i_d(
     id string  comment '任务节点ID',
     task_instance_id string  comment '任务实例ID',
     map_code string  comment '地图编码',
     map_name string comment '地图名称',
     map_floor int comment '地图楼层',
     point_name string comment '任务点名称',
     point string comment '任务点坐标',
     point_type int comment '任务点类型，普通点、充电桩、电梯内点等',
     yaw string comment '偏航角',
     target_point_flag string comment '目标点关键节点标识，0-非目标点 1-目标点',
     target_point_name string comment '目标点名称',
     status int comment '任务点状态，0：未开始 1：进行中  3：异常结束  4：成功完成',
     error_code string comment '错误码',
     error_message string comment '错误信息',
     error_detail string comment '错误详情信息',
     actions string comment '指令数据',
     ext_data string comment '扩展数据',
     sort_no int comment '排序号',
     start_time bigint comment '开始时间',
     end_time bigint comment '结束时间',
     create_time bigint comment '创建时间',
     update_time bigint comment '更新时间',
     version int comment '乐观锁版本号',
     bigdata_method string comment 'db操作类型:INSERT;UPDATE;DELETE',
     k8s_env_name string  comment '环境名称'
)
comment 'jinger任务日志crss_task.t_task_node_log'
PARTITIONED BY (dt string COMMENT '日期')
STORED AS parquet
location '/data/cdmods/ods_crio_db_c0033_t_task_node_log_i_d';