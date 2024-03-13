-- cdmdwd.dwd_roc_pmd_user_rcu_robot_a_d  user-rcu-robot关联信息表
create external table cdmdwd.dwd_roc_pmd_user_rcu_robot_a_d(
     robot_id string comment '机器人id',
     tenant_id string comment '租户id',
     robot_account_id string comment '机器人账户id',
     rcu_id string comment 'rcu id',
     token string comment 'token值',
     status int comment '状态值: -1-删除; 0-注册未激活; 1-激活可用; 2-通知VPN注册',
     status_name String comment '状态名称',
     db_op int comment '操作类型 1:insert 2:update 3:delete',
     event_time string comment '事件时间',
     k8s_env_name string comment '数据来源环境'
)
comment 'user-rcu-robot关联信息表'
stored as parquet;