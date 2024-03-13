CREATE EXTERNAL TABLE cdmdim.dim_umd_robot_account_sh_d (
    robot_account_id string comment '用户登录ID',
    robot_account_name string comment '姓名',
    email string comment '邮件地址',
    phone string comment '手机号码',
    status int comment '有效生命周期内用户状态,即机器人账户: -1-删除,0-正常,1-停用',
    status_name string comment '有效生命周期内用户状态,即机器人账户: -1-删除,0-正常,1-停用',
    privacy_type int comment '隐私类型: 1-公共, 2-私有',
    privacy_type_name string comment '隐私类型: 1-公共, 2-私有',
    is_hi_service int comment '人工服务: 0-禁止，1-允许',
    is_privacy_enhance int comment '隐私增强：0-关闭，1-开启',
    is_auto_takeover int comment '允许自动接管: 0-禁止，1-允许',
    app_id string comment 'app应用id',
    video_mode string comment '视频模式：videocall, videoroom',
    phone_number_prefix string comment '手机号码前缀',
    tenant_id string comment '租户code码',
    operator_id string comment '操作员ID', 
    ross_credential_id string comment 'idoe中的ross-credential-id', 
    start_time string comment '拉链信息开始时间',
    end_time string comment '拉链信息结束时间',
    k8s_env_name string comment '环境名',
    create_time string comment '创建时间',
    update_time string comment '更新时间'
)
comment '机器人账户信息表,状态非-1,租户与机器人账户确定唯一,业务规则：机器人非-1状态下,租户下的机器人账户唯一'
PARTITIONED BY (dt string comment '日期')
STORED AS PARQUET
location '/data/cdmdim/dim_umd_robot_account_sh_d';