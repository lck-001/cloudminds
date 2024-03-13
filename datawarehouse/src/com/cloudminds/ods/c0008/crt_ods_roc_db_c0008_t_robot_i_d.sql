create external table cdmods.ods_roc_db_c0008_t_robot_i_d (
    id string comment 'robot_ID',
    tenant_code string comment '隶属租户code',
    robot_code string comment 'robot唯一标识值',
    robot_name string comment 'robot名称',
    robot_type bigint comment 'robot类型: 1-Pepper, 2-META, 3-Patrol, 4-Ginger, 5-NUC, 6-PAD,DEFAULT 1',
    manufacturer string comment 'robot厂商',
    model string comment 'robot型号',
    sku string comment 'robot sku',
    os string comment '操作系统平台',
    version_code string comment ' 版本号',
    boot_time string comment '开机时长',
    last_online_time string comment '最后在线时间',
    online_flag bigint comment '在线状态: 0-不在线, 1-在线,DEFAULT 0',
    service_flag bigint comment '0:初始值,1: AI服务中,2:人工服务中,3:服务中断,DEFAULT 0',
    service_seat string comment '服务坐席',
    alarm_lastest string comment '最新告警信息',
    head_url string comment '展示头像',
    confidence_index bigint comment '服务指数 DEFAULT 100',
    status bigint comment 'robot状态: -1-删除,0-正常,1-停用,DEFAULT 0',
    regist_time string comment '注册时间',
    update_time string comment 'robot信息更新时间',
    poweron_time string comment '开机时间',
    poweroff_time string comment '关机时间',
    description string comment '描述信息',
    remind_policy_id string comment '提醒策略ID',
    bind_status bigint comment '机器人受控状态: -1-删除, 0-注册未激活, 1-激活可用, 2-通知VPN注册, DEFAULT 0',
    user_code string comment '用户登录ID',
    user_name string comment '账号姓名',
    service_code string comment '服务code',
    privacy_enhance bigint comment '隐私增强：0-关闭，1-开启,DEFAULT 0',
    rcu_code string comment 'RCU唯一标识rcuId值',
    hi_service bigint comment '人工服务: 0-禁止，1-允许, DEFAULT 1',
    event_time BIGINT comment '事件发生时间',
    bigdata_method string comment 'db操作类型:INSERT;UPDATE;DELETE',
    k8s_env_name string  comment '环境名称'
)
comment 'roc系统robot信息表,roc.t_robot'
PARTITIONED BY (dt string comment '日期')
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
STORED AS TEXTFILE
location '/data/cdmods/ods_roc_db_c0008_t_robot_i_d';