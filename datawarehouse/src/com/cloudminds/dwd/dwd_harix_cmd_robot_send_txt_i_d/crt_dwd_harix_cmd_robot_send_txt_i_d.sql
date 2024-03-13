-- cdmdwd.dwd_cmd_harix_robot_send_txt_i_d robot skill发送识别的quest_text到harix_switch
create external table cdmdwd.dwd_harix_cmd_robot_send_txt_i_d (
     question_id string comment '请求id',
     robot_id string comment '机器人id',
     rcu_audio string comment '机器人录制的RCU声音文件',
     robot_account_id string comment '机器人账户，roc系统中创建的机器人账号',
     service_code string comment '机器人和客服服务码',
     tenant_id string comment '租户id',
     robot_type string comment '机器人类型',
     rod_type string comment '事件类型',
     harix_switch_id string comment 'hari switch的id',
     rcu_id string comment 'rcu id',
     type_url string comment '请求的url类型',
     qa_flag string comment 'qa问题来源,HI CLOUD',
     question_text string comment '识别出的文本',
     question_language string comment '请求的语言类型',
     msg_from string comment '消息发出的模块',
     msg_send_name string comment '消息发出的用户名',
     msg_id string comment '消息id',
     msg_receive_name string comment '收消息的用户名',
     msg_type string comment 'switch交换quest类型,消息的type',
     msg_dest string comment 'switch交换的目的地,收消息的模块名',
     k8s_svc_name string comment 'k8s smart voice control 名称',
     k8s_env_name string comment 'k8s环境名称',
     event_time string comment '事件发生时时间',
     ext string comment '保留的原始数据'
)
comment 'robot skill发送识别的quest_text到harix_switch'
partitioned by(dt string comment 'dt分区字段')
stored as parquet
location '/data/cdmdwd/dwd_harix_cmd_robot_send_txt_i_d';