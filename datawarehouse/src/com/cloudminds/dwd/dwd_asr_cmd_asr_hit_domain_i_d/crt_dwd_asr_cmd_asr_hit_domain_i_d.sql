-- cdmdwd.dwd_asr_cmd_asr_hit_domain_i_d asr命中asr_domain
create external table cdmdwd.dwd_asr_cmd_asr_hit_domain_i_d (
     question_id string comment '请求id',
     robot_id string comment '机器人id',
     robot_account_id string comment '机器人账户，roc系统中创建的机器人账号',
     version string comment '当前版本号',
     service_code string comment '机器人和客服服务码',
     tenant_id string comment '租户id',
     robot_type string comment '机器人类型',
     rod_type string comment '事件类型',
     question_text string comment '识别的文本,识别的文本为空也不会请求nlp',
     asr_domain string comment 'asr domain',
     asr_third_total int comment '第三方asr识别时间,最后一个Pre END开始到返回最终文本结束',
     asr_vendor string comment 'asr提供商',
     asr_start string comment 'asr识别开始时间',
     asr_end string comment 'asr识别完成时间',
     nlufwd string comment '转发到下一个应用,调用nlu或转发方式,空串 回显,reception,service app,nlu',
     sv_agent_id string comment 'smart voice 的agent id',
     wake_status int comment '-1:异常错误,0:休眠状态,1:正常，0 状态不精准,此状态下不会请求nlp,-1 可能会请求,1:唤醒,会请求nlp',
     wake_status_name string comment '-1:异常错误,0:休眠状态,1:正常，0/-1 状态不精准,此状态下不会请求nlp,1:唤醒,会请求nlp',
     nlp_total int comment 'asrctrl 请求nlp处理,nlp总耗时',
     log_module string comment '上传日志的模块',
     gap int comment '音频上传结束到asr识别完成的时间',
     vocal_total int comment '声纹识别耗时',
     last_pre_end_start string comment '最后一次pre end开始时间',
     asrctrl_duration int comment '整个请求中asr 耗时时长,asr持续时间',
     x_b3_traceid string comment 'k8s中带的x-b3-traceid',
     asrctrl_inner int comment 'asrctrl 内部耗时 (去掉asr、nlu、转发答案等剩余的asrctrl耗时)',
     caller string comment '回调方法',
     is_pre_end int comment '是否有pre end',
     vpr_id string comment '声纹id',
     asrctrl_robot_skill_total int comment 'asrctrl调用robotskill的总耗时（包含了handleAction发送答案的耗时）',
     asr_nlp_total int comment 'ASR+NLP主要业务耗时',
     audio_size int comment '音频大小字节',
     k8s_svc_name string comment 'k8s smart voice control 名称',
     k8s_env_name string comment 'k8s环境名称',
     event_time string comment '事件发生时时间',
     ext string comment '保留的原始数据'
)
comment 'asr识别录制的声音'
partitioned by(dt string comment 'dt分区字段')
stored as parquet
location '/data/cdmdwd/dwd_asr_cmd_asr_hit_domain_i_d';