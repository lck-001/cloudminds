create external table cdmdwd.dwd_cms_svo_asr_label_event_a_d(
    event_type_id string comment '事件编码的值',
    event_name string comment '事件名称',
    event_time string comment '事件时间,精确到毫秒(北京时间)',
    model_id string comment '模块id',
    tenant_id string comment '租户id',
    option string comment '扩展字段',
    operator_id int comment '标注人ID',
    project_id int comment '项目ID',
    label_type_id string comment '标签类型ID 1.音频可用 2. 音频不可用 3. 音频备用',
    -- label_type_name string comment '标签类型描述',
    label_id string comment '标签ID',
    label_name string comment '标签名',
    question_id string comment '端到端对话 的chuanID',
    asr_correct_text string comment 'asr修正内容',
    audio_correct_text string comment '粗标修正内容',
    submit_time string comment '标注批次任务提交时间',
    k8s_env_name string comment 'k8s环境名称'
)
comment 'asr标注事件明细表'
stored as parquet;

