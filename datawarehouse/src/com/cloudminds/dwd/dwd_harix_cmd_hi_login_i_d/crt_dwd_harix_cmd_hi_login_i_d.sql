create external table cdmdwd.dwd_harix_cmd_hi_login_i_d (
    hi_id string comment '坐席id',
    account string comment '用户登录id',
    event_time string comment '事件时间',
    db_op int comment '数据库操作：1：insert  2:update 3:delete',
    k8s_env_name string comment '环境名称'
)comment '坐席登录和下线表， 出现在表中即表示登陆过'
partitioned by(dt string comment '日期')
STORED AS parquet;