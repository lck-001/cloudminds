-- cdmdwd.dwd_harix_cmd_hi_intelligence_i_d hi辅助增强
create external table cdmdwd.dwd_harix_cmd_hi_intelligence_i_d (
     question_id string comment '请求id',
     robot_id string comment '机器人id',
     rcu_id string comment 'rcu id',
     robot_account_id string comment '机器人账户，roc系统中创建的机器人账号',
     service_code string comment '机器人和客服服务码',
     tenant_id string comment '租户id',
     robot_type string comment '机器人类型',
     rod_type string comment '事件类型',
     hi_account string comment '坐席账号',
     msg string comment '数据内容,对应日志中eventContent',
     msg_from string comment '数据内容来源，hiChatListConfirmSimQ-确认问题 答案中的相似问题,hiChatListConfirmGW-确认问题  GW 中的问题,hiChatListConfirmAiAnswer-确认答案 有无信心都可点击,hiQuestionPanel - 输入框输入的问题,hiAnswerPanel - 输入框输入的答案,hiDancePanel - 常用舞蹈  对应上面isAction 为true,hiActionPanel - 常用动作  对应上面isAction 为true,hiShortcutPanel - 自定义面板中  自定义按钮，包括问题和答案,hiPhaseList - 常用短语',
     msg_from_name string comment '数据内容来源，hiChatListConfirmSimQ-确认相似问题,hiChatListConfirmGW-确认问题gateway问题,hiChatListConfirmAiAnswer-确认答案 ,hiQuestionPanel - 输入框输入问题,hiAnswerPanel - 输入框输入答案,hiDancePanel - 常用舞蹈  对应上面isAction 为true,hiActionPanel - 常用动作  对应上面isAction 为true,hiShortcutPanel - 自定义按钮，包括问题和答案,hiPhaseList - 常用短语',
     msg_type string comment 'question、answer、action',
     k8s_svc_name string comment 'k8s smart voice control 名称',
     k8s_env_name string comment 'k8s环境名称',
     event_time string comment '事件发生时时间',
     ext string comment '保留的原始数据'
)
comment 'hi辅助增强'
partitioned by(dt string comment 'dt分区字段')
stored as parquet
location '/data/cdmdwd/dwd_harix_cmd_hi_intelligence_i_d';