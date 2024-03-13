create external table cdmdwd.dwd_cms_svo_anno_label_event_a_d(
    event_type_id string comment '事件编码的值',
    event_name string comment '事件名称',
    event_time string comment '事件时间,精确到毫秒(北京时间)',
    model_id string comment '模块id',
    tenant_id string comment '租户id',
    option string comment '扩展字段',
    operator_id int comment '标注人ID',
    project_id int comment '项目ID',
    label_type_id string comment '标签类型ID ，暂时 1.听写错误/回答正确 2. 听写错误/回答错误 3. 听写正确/回答错误 4.HA介入 5. 杂音 6. 断句错误 7. 人工巡检 8 听写和回答都没问题',
    label_type_name string comment '标签类型描述',
    label_id string comment '标签ID',
    label_name string comment '标签名',
    question_id string comment '端到端对话 的chuanID',
    correct_text string comment '针对标注错误的标签，填写修正值',
    submit_time string comment '标注批次任务提交时间',
    k8s_env_name string comment 'k8s环境名称'
)
comment 'cms 标注事件明细表'
stored as parquet;

