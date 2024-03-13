CREATE EXTERNAL TABLE `cdmods.ods_roc_db_c0002_t_user_s_d`(
  `id` string COMMENT '用户信息ID', 
  `tenant_code` string COMMENT '隶属租户code', 
  `user_code` string COMMENT '用户登录ID', 
  `pwd` string COMMENT '登录密码', 
  `user_name` string COMMENT '姓名', 
  `email` string COMMENT '邮件地址', 
  `phone_code` string COMMENT '手机号码', 
  `status` int COMMENT '用户状态: -1-删除\;0-正常\;1-停用', 
  `create_time` string COMMENT '创建时间', 
  `update_time` string COMMENT '更新时间', 
  `operator_id` string COMMENT '操作员ID', 
  `service_code` string COMMENT '服务code', 
  `privacy_type` int COMMENT '隐私类型: 1-公共\; 2-私有', 
  `hi_service` int COMMENT '人工服务: 0-禁止，1-允许', 
  `privacy_enhance` int COMMENT '隐私增强：0-关闭，1-开启', 
  `auto_takeover` int COMMENT '允许自动接管: 0-禁止，1-允许', 
  `robot_type` int COMMENT '机器人类型: 1-Pepper\; 2-META\; 3-Patrol\; 4-Ginger\; 5-NUC\; 6-PAD', 
  `app_id` string COMMENT 'app应用id', 
  `video_mode` string COMMENT '视频模式：videocall\; videoroom', 
  `ross_credential_id` string COMMENT 'idoe中的ross-credential-id', 
  `phone_number_prefix` string COMMENT '手机号码前缀',
  k8s_env_name string COMMENT '环境名'
)
PARTITIONED BY(`dt` STRING)
row format delimited fields terminated by '\001'
STORED AS PARQUET;