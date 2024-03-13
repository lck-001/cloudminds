-- cdmdwd.dwd_harix_cmd_reception_send_qa_i_d reception发送qa(question&answer\action)到robot skill
create external table cdmdwd.dwd_harix_cmd_reception_send_qa_i_d (
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
     answer_text string comment 'nlp响应的文本内容',
     answer_language string comment 'nlp响应的内容语言类型',
--     sv_agent_id string comment 'smart voice 的agent id',
--     sv_agent_name string comment 'smart voice 的agent名称',
     tts_step string comment '当前question的tts步骤号,使用1,2,3表示',
     intent_name string comment '意图名称,可以是action code,例如握手，也可以是其他意图',
     tts_emoji string comment 'tts emoji 表情',
     tts_payload string comment 'tts有效数据',
     action_duration string comment 'tts 播放持续时间',
     action_frame_no string comment '帧数',
     action_play_type string comment 'action 播放方式',
     action_video_url string comment 'action 相关视频url',
     action_guide_tip string comment '引导提示',
     action_pic_url string comment 'action 图片地址',
     action_intent string comment 'nlp处理后的意图',
     action_url string comment 'action 所对应资源的url',
     action_display string comment 'tts 显示？',
     action_name string comment 'action 名称',
     tts_audio string comment 'tts 音频？',
     tts_type string comment 'tts 类型',
     k8s_svc_name string comment 'k8s smart voice control 名称',
     k8s_env_name string comment 'k8s环境名称',
     event_time string comment '事件发生时时间',
     ext string comment '保留的原始数据'
)
comment 'reception发送qa(question&answer/action)到robot skill'
partitioned by(dt string comment 'dt分区字段')
stored as parquet
location '/data/cdmdwd/dwd_harix_cmd_reception_send_qa_i_d';