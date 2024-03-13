create external table cdmdwd.dwd_harix_cmd_hi_service_i_d (
    hi_id string comment '坐席id',
    account string comment '用户登录账户',
    service_code string comment '服务编码',
    event_time string comment '事件时间',
    db_op int comment '数据库操作：1：insert  2:update 3:delete',
    k8s_env_name string comment '环境名称'
)comment '坐席服务编码与id,account对应关系'
partitioned by(dt string comment '日期')
STORED AS parquet;