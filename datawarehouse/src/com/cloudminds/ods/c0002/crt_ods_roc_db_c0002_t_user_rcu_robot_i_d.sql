-- cdmods.ods_roc_db_c0002_t_user_rcu_robot_i_d user-rcu-robot关联信息表
create external table cdmods.ods_roc_db_c0002_t_user_rcu_robot_i_d(
     id string comment 'id值',
     tenant_code string comment '租户id',
     user_id string comment '用户id',
     rcu_id string comment 'rcuid',
     robot_id string comment '机器人id',
     user_code string comment '用户登录id',
     rcu_code string comment 'RCU唯一标识rcuId值',
     robot_code string comment 'robot唯一标识值',
     token string comment 'token值',
     status tinyint comment '关联信息状态: -1-删除; 0-注册未激活; 1-激活可用; 2-通知VPN注册',
     event_time bigint comment '事件发生时间',
     bigdata_method string comment 'db操作类型:INSERT;UPDATE;DELETE',
     create_time string comment '创建时间',
     update_time string comment '修改时间'
)
comment 'user-rcu-robot关联信息表roc.t_user_rcu_robot'
PARTITIONED BY (dt string COMMENT '日期')
STORED AS parquet;