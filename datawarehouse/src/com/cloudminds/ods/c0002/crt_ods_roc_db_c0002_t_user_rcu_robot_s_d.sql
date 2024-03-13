-- cdmods.crt_ods_roc_db_c0002_t_user_rcu_robot_s_d user-rcu-robot关联信息表
create external table cdmods.ods_roc_db_c0002_t_user_rcu_robot_s_d(
     id string comment 'RCU_ID',
     tenant_code string comment '隶属租户code',
     user_id string comment '用户ID',
     rcu_id string comment 'RcuID',
     robot_id string comment 'RobotID',
     user_code string comment '用户登录ID',
     rcu_code string comment 'RCU唯一标识rcuId值',
     robot_code string comment 'robot唯一标识值',
     token string comment 'token值',
     status int comment '关联信息状态: -1-删除; 0-注册未激活; 1-激活可用; 2-通知VPN注册',
     create_time string comment '创建时间',
     update_time string comment '更新时间',
     k8s_env_name string comment '环境名称'
)
comment 'user-rcu-robot关联信息表 roc.t_user_rcu_robot'
PARTITIONED BY (dt string comment '日期')
STORED AS parquet;