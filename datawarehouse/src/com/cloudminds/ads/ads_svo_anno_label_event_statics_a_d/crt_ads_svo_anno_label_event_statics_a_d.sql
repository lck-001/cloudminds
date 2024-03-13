create external table cdmads.ads_svo_anno_label_event_statics_a_d (
    -- cdmdwm.dwm_svo_anno_label_event_i_d
    robot__robot_id string comment '机器人id标识，既是robot_code',
    robot__robot_name string comment '机器人名称',
    robot__robot_type_id string comment '机器人类型',
    robot__robot_type_inner_name string comment '机器人类型名(研发)',
    robot__robot_type_name string comment '机器人类型名',
    sv_agent_id string comment 'smart voice 的agent id',
    sv_agent_name string comment 'smart voice 的agent名称',
    nlu_event_date string comment 'nlu事件发生时时间',
    simple_label_noise_cnt int comment '粗标杂音数',
    valid_conversation_cnt int comment '有效交互数',
    expected_ai_ha_conversation_cnt int comment 'AI+HA对话端到端满足数（ai回答正确+HA介入的数）',
    expected_ai_conversation_cnt int comment 'AI对话端到端满足数（ai回答正确对话 ）',
    asr_recognize_correct_cnt int comment 'AI下语音识别满足数（asr 识别音频数）',
    expected_answer_cnt int comment 'AI下语义对话满足数分子(符合期望的回答数)',
    expected_answer_cnt_denominator int comment 'AI下语义对话满足数分母（全部正确+听写正确,答案错误+听写错误,答案正确）',
    
    sla_cnt int comment '用户内容准确数分子',
    sla_cnt_denominator int comment '用户内容准确数分母',
    -- cdmdws.dws_omd_robot_statistic_i_d
    robot_online_time int comment '机器人在线时长',
    -- cdmdwm.dwm_cmd_audio_process_i_d
    asr_filter_cnt int comment 'asr_filter数据量',

    --DATA-82新增需求
    robot__sku string comment '机器人SKU',
    robot__version_code string comment '版本号',
    robot__software_version string comment '机器人软件版本',
    robot__hardware_version string comment '机器人硬件版本',

    ha_cnt int comment 'HA介入数量',

    --sv项目看板新增需求
    trainer_id string comment '训练师id',
    trainer_name string comment '训练师名称',
    operator_id string comment '项目运营者id',
    operator_name string comment '项目运营者名称',
    project_name string comment '项目名称',
    priority string comment '项目优先级 p0 p1 p2 p3',
    content_operator_id string comment '内容运营者id',
    content_operator_name string comment '内容运营者名称',
    category string comment '项目类别 长租 短租 售卖 展会演示 客户演示 达闼展厅',
    scene string comment '场景展厅 展会 通用 商业中心/综合体 物业 售楼处 图书馆 手机卖场 移动/电信等营业厅 银行网点 办证中心 酒店 机场 地铁 游客中心/旅游推荐 医院 交警车管所 法院 政务中心 保险营业厅 校园 汽车4S店 博物馆',

    --DATA-100 & DATA-105
    status int comment '运营状态 0-未开始 1-在运营 2-到期关闭 3-到期未关闭 4-已取消',
    status_name string comment '运营状态名称 0-未开始 1-在运营 2-到期关闭 3-到期未关闭 4-已取消',
    user_qa_new_cnt int comment 'user_qa每日新增数量',
    user_qa_total_cnt int comment 'user_qa总量',

    -- DATA-142
    tenant__tenant_id string comment '租户id'
)
comment '粗标事件统计指标表'
stored as parquet;