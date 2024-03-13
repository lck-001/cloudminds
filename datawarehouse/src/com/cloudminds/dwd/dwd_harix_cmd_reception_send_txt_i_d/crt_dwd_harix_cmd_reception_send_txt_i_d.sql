-- cdmdwd.dwd_harix_cmd_reception_send_txt_i_d reception skill发送识别的quest_text到robot skill
create external table cdmdwd.dwd_harix_cmd_reception_send_txt_i_d (
     question_id string comment '请求id',
     robot_id string comment '机器人id',
     rcu_audio string comment '机器人录制的RCU声音文件',
     robot_account_id string comment '机器人账户，roc系统中创建的机器人账号',
     service_code string comment '机器人和客服服务码',
     tenant_id string comment '租户id',
     robot_type string comment '机器人类型',
     rod_type string comment '事件类型',
     qa_flag string comment 'qa问题来源,HI CLOUD',
     question_text string comment '识别出的文本',
     question_language string comment '请求的语言类型',
     send_delay int comment 'reception发送到robot skill时延',
     k8s_svc_name string comment 'k8s smart voice control 名称',
     k8s_env_name string comment 'k8s环境名称',
     event_time string comment '事件发生时时间',
     ext string comment '保留的原始数据'
)
comment 'reception skill发送识别的quest_text到robot skill'
partitioned by(dt string comment 'dt分区字段')
stored as parquet
location '/data/cdmdwd/dwd_harix_cmd_reception_send_txt_i_d';