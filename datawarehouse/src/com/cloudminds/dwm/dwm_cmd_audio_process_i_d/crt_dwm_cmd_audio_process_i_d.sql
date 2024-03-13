-- cdmdwd.dwm_cmd_audio_process_i_d audio全流程宽表
create external table cdmdwm.dwm_cmd_audio_process_i_d (
    question_id string comment '请求id',
    -- customer
    customer__customer_id string comment '客户id',
    customer__customer_name string comment '客户名称',
    customer__pid string comment '客户父级',
    customer__level string comment '当前客户层级',
    customer__region string comment '客户所在区域',
    customer__address string comment '客户地址',
    customer__website string comment '客户网址',
    customer__industry_id string comment '客户所属行业编码',
    customer__industry_name string comment '客户所属行业编码',
    customer__nature_code string comment '客户性质编码',
    customer__nature_name string comment '客户性质名称',
    customer__credit_code string comment '客户信用编码',
    customer__credit_name string comment '客户信用名称',
    customer__source_code string comment '客户来源编码',
    customer__source_name string comment '客户来源名称',
    customer__scale_code string comment '公司规模编码',
    customer__scale_name string comment '公司规模名称',
    customer__status_code string comment '客户行业地位编码',
    customer__status_name string comment '客户行业地位名称',
    customer__contact_code string comment '联系策略编码',
    customer__contact_name string comment '联系策略名称',
    customer__purchase_code string comment '客户购买策略编码',
    customer__purchase_name string comment '客户购买策略名称',
    customer__employment_code string comment '客户从业时间编号',
    customer__employment_name string comment '客户从业时间名称',
    customer__settlement_code string comment '结算方式编码',
    customer__settlement_name string comment '结算方式名称',
    customer__employer_num int comment '客户员工数量',
    customer__category_code string comment '客户类别编码',
    customer__category_name string comment '客户类别名称',
    customer__create_time string comment '客户创建时间',

    -- 租户
    tenant__tenant_id string comment '租户id',
    tenant__tenant_name string comment '租户名称',
    tenant__tenant_type string comment '租户类型',
    tenant__industry_id string comment '租户所属行业id',
    tenant__industry_name string comment '租户所属行业id',
    tenant__sub_sys string comment '子系统',
    tenant__time_zone string comment '时区',
    tenant__net_env_id string comment '网络环境',
    tenant__mcs_area string comment 'mcs区域',
    tenant__device_num int comment '最大设备数',
    tenant__region string comment '地区',
    tenant__address string comment '地址',
    tenant__contact string comment '租户联系人',
    tenant__phone string comment '联系电话',
    tenant__telephone string comment '联系电话',
    tenant__seller string comment '销售人员',
    tenant__email string comment '邮箱地址',
    tenant__status int comment '租户状态: -1-删除;0-正常;1-待开通;2-锁定',
    tenant__status_name string comment '租户状态',
    tenant__charge_time string comment '计费开始时间',
    tenant__expired_time string comment '租户过期时间',
    tenant__logo string comment '租户logo地址',
    tenant__description string comment '租户简要描述',
    tenant__brand string comment '客户品牌化参数',
    tenant__is_need_vpn int comment '是否需要VPN: 0-不需要,1-需要',
    tenant__create_time string comment '租户创建时间',

     -- robot
    robot__robot_id string comment '机器人id标识，既是robot_code',
    robot__robot_name string comment '机器人名称',
    robot__robot_type_id string comment '机器人类型',
    robot__robot_type_inner_name string comment '机器人类型名(研发)',
    robot__robot_type_name string comment '机器人类型名',
    robot__asset_code string comment '资产code',
    robot__asset_type string comment '资产类型',
    robot__product_type_code string comment '产品类型code',
    robot__product_type_code_name string comment '产品类型code所对应的名称',
    robot__product_id string comment '产品ID',
    robot__product_id_name string comment '产品名字',
    robot__assets_status int comment '资产状态 0-在册 1-在库 2-出库',
    robot__assets_status_name string comment '资产状态 在册 在库 出库',
    robot__status int comment '机器人状态 robot状态: -1-删除 0-正常 1-停用',
    robot__status_name string comment '机器人状态',
    robot__robot_manufacturer_id string comment '机器人制造商id',
    robot__robot_manufacturer_name string comment '机器人制造商名称',
    robot__model string comment 'robot型号,roc中model boss中device_model',
    robot__os string comment '操作系统平台',
    robot__version_code string comment '版本号',
    robot__software_version string comment '机器人软件版本',
    robot__hardware_version string comment '机器人硬件版本',
    robot__head_url string comment '展示头像',
    robot__regist_time string comment '注册时间',
    robot__bind_status int comment '机器人受控状态: -1-删除 0-注册未激活 1-激活可用 2-通知VPN注册',
    robot__bind_status_name string comment '机器人受控状态名',
    robot__is_hi_service int comment '人工服务: 0-禁止，1-允许',
    robot__is_privacy_enhance int comment '隐私增强：0-关闭，1-开启',
    robot__remind_policy_id string comment '提醒策略id',
    robot__sku string comment '机器人SKU',
    robot__quality_date string comment '供应商质保期',
    robot__customer_quality_date string comment '客户质保期',
    robot__out_type_state int comment '出库类型 0 交付出库 1 维修出库 2 报废出库 3 交付给客户 4 交付给测试',
    robot__out_type_state_name string comment '出库类型 0 交付出库 1 维修出库 2 报废出库 3 交付给客户 4 交付给测试',
    robot__product_date string comment '生产日期',
    robot__in_stock_date string comment '入库日期',
    robot__out_stock_date string comment '出库日期',
    robot__in_stock_staff_id string comment '入库操作人员ID',
    robot__in_stock_staff_name string comment '入库操作人员姓名',
    robot__out_stock_staff_id string comment '出库操作人员ID',
    robot__out_stock_staff_name string comment '出库操作人员姓名',
    robot__robot_note string comment '备注',
    robot__description string comment '机器人信息描述',
    robot__create_time string comment '机器人创建时间',


     --rcu
    rcu__rcu_id string comment 'rcu id',
    rcu__rcu_name string comment 'rcu名称',
    rcu__imei string comment 'rcu imei码',
    rcu__did string COMMENT 'Decentralized Identifier,去中心化的身份id',
    rcu__did_credential_id string COMMENT 'idoe中的did-credential-id',
    rcu__rcu_manufacturer_name string comment 'rcu制造商',
    rcu__model string comment 'rcu模式',
    rcu__os string comment 'rcu os版本',
    rcu__version_code string comment 'rcu版本号',
    rcu__status int COMMENT 'RCU状态: -1-删除,0-正常,1-停用',
    rcu__status_name string comment 'RCU状态: -1-删除,0-正常,1-停用',
    rcu__cpu string comment 'cpu信息',
    rcu__ram string comment 'ram信息',
    rcu__rom_capacity string comment 'rom总容量',
    rcu__rom_available_capacity string comment 'rom可用空间',
    rcu__sd_capacity string comment 'sd卡容量',
    rcu__sd_available_capacity string comment 'sd卡可用空间',
    rcu__camera string comment '摄像头',
    rcu__wifi_mac string comment 'wifi mac地址',
    rcu__bluetooth_mac string comment '蓝牙mac地址',
    rcu__regist_time string COMMENT '注册时间',
    rcu__activate_time string COMMENT '激活时间',
    rcu__deactivate_time string COMMENT '取消激活时间',
    rcu__description STRING COMMENT '描述信息',
    rcu__create_time string comment 'rcu创建时间',

     -- robot_account_id
    robot_account__robot_account_id string comment '机器人账户，roc系统中创建的机器人账号',
    robot_account__robot_account_name string comment '姓名',
    robot_account__email string comment '邮件地址',
    robot_account__phone string comment '手机号码',
    robot_account__status int comment '有效生命周期内用户状态,即机器人账户: -1-删除,0-正常,1-停用',
    robot_account__status_name string comment '有效生命周期内用户状态,即机器人账户: -1-删除,0-正常,1-停用',
    robot_account__privacy_type int comment '隐私类型: 1-公共, 2-私有',
    robot_account__privacy_type_name string comment '隐私类型: 1-公共, 2-私有',
    robot_account__is_hi_service int comment '人工服务: 0-禁止，1-允许',
    robot_account__is_privacy_enhance int comment '隐私增强：0-关闭，1-开启',
    robot_account__is_auto_takeover int comment '允许自动接管: 0-禁止，1-允许',
    robot_account__app_id string comment 'app应用id',
    robot_account__video_mode string comment '视频模式：videocall, videoroom',
    robot_account__phone_number_prefix string comment '手机号码前缀',
    robot_account__operator_id string comment 'roc操作人员id',
    robot_account__ross_credential_id string comment 'idoe中的ross-credential-id',
    robot_account__create_time string comment '机器人账户创建时间',

    -- hi
    hi__hi_account string comment '坐席账户,即login_id',
    hi__hi_name string comment '坐席名称',
    hi__hi_email string comment '坐席邮箱,绑定的邮箱地址',
    hi__hi_phone string comment '联系电话',
    hi__load_type int comment '坐席来源,0-手工录入,1-文件导入,2-ad导入,3-ad同步',
    hi__load_type_name string comment '坐席来源,0-手工录入,1-文件导入,2-ad导入,3-ad同步',
    hi__operator_id string comment '操作员id',
    hi__hi_group string comment '坐席分组id',
    hi__create_time string comment '坐席创建时间',

     -- sv_agent_id 无法关联
--     sv_name string comment 'SV配置名称',
--     sv_agent_id string comment 'NLP库的AgentID',
--     sv_agent_name string comment 'NLP库的AgentName',
--     is_built_in int comment '是否内置',
--     is_del int comment '是否删除',

     -- action
    intent__intent_id string comment 'intent id',
    intent__intent_name string comment 'intent 名称',
    intent__nlp_agent_id string comment 'agent id',
    intent__input_context string comment '输入上下文',
    intent__output_context string comment '输出上下文',
    intent__voice_pad string comment '语音垫片',
    intent__prompt string comment '提示词',
    intent__action string COMMENT 'action',
    intent__df_intent_id string comment 'dialog flow 意图id',
    intent__is_lua_enabled int COMMENT '是否开启lua脚本 1启用，0关闭',
    intent__reply int COMMENT '是否开启回答 1启用，0关闭',
    intent__create_time string comment '意图创建时间',

    -- domain
    domain__domain_id string comment 'domain id',
    domain__domain_name string COMMENT 'domain 名称',
    domain__domain_info string COMMENT 'domain 信息',
    domain__domain_type string COMMENT 'domain类型 乱码 废弃字段',
    domain__domain_url string COMMENT 'URL',
    domain__call_service string COMMENT '调用接口',
    domain__is_release int COMMENT '是否发布 1-发布,0-未发布',
    domain__is_closed_recg int COMMENT '关闭后是否意图识别, 1-关闭后能意图识别,0-关闭后不能意图识别',
    domain__closed_hit string COMMENT '关闭后命中话术，多句回复用&&隔开',
    domain__keyword int COMMENT '1：代表走关键词服务，此时服务在redis中算是停用状态 其他：不走服务 ',
    domain__domain_switch_hit string COMMENT '??????',
    domain__create_time string comment 'domain创建时间',

    -- fqa
    fqa_cate__fqa_cate_id string comment 'ID',
    fqa_cate__pid string comment '父id',
    fqa_cate__cate_id_path string comment '分类id path',
    fqa_cate__cate_name_path string comment '分类cate name path',
    fqa_cate__cate_name string comment '分类名字',
    fqa_cate__level int comment 'level',
    fqa_cate__is_work int comment '是否生效 0:no 1:yes',
    fqa_cate__is_work_name string comment '是否生效 0:no 1:yes',
    fqa_cate__push_action_name string comment '推送动作 为了推送给kafka add delete update',
    fqa_cate__is_need_push int comment '是否需要推送kafka 0:yes 1:no',
    fqa_cate__is_del int comment '是否删除 0:no 1:yes',
    fqa_cate__last_seq_id string comment 'kafka seq_id',
    fqa_cate__audit_name string comment '记录操作轨迹',
    fqa_cate__sv_msg string comment '记录同步轨迹',
    fqa_cate__source string comment '来源',
    fqa_cate__create_time string comment '添加时间',

    -- user_qa
    user_qa_cate__user_qa_cate_id string comment 'ID',
    user_qa_cate__pid string comment '父id',
    user_qa_cate__cate_id_path string comment '分类id path',
    user_qa_cate__cate_name_path string comment '分类cate name path',
    user_qa_cate__cate_name string comment '分类名字',
    user_qa_cate__level int comment 'level',
    user_qa_cate__is_work int comment '是否生效 0:no 1:yes',
    user_qa_cate__is_work_name string comment '是否生效 0:no 1:yes',
    user_qa_cate__audit_name string comment '记录操作轨迹',
    user_qa_cate__agent_id string comment 'sv 的 agent id',
    user_qa_cate__source string comment '来源',
    user_qa_cate__create_time string comment '添加时间',


    -- 机器人录制声音
    rcu_audio string comment '机器人录制的RCU声音文件',
    is_noise int comment '是否是噪音 1-表示噪音，0-表示不是噪音',
    service_code string comment '机器人和客服服务码',
    app_type string comment '机器人RCU中安装的app类型',
    robot_type string comment '机器人类型',
    audio_record_type string comment 'audio声音录制类型 STREAMING',
    qa_flag string comment 'qa问题来源,HI CLOUD',
    duration int comment '录音持续时间，毫秒',
    dialect string comment '本地方言',
    audio_sample_rate string comment '音频采样率',
    asr_vendor string comment 'asr提供商',
    format string comment '格式',
    channel string comment '频道',

    rcu_record_k8s_svc_name string comment 'k8s smart voice control 名称',
    rcu_record_k8s_env_name string comment 'k8s环境名称',
    rcu_record_event_time string comment '事件发生时时间',
    is_rcu_record int comment 'rcu_record 是否录制声音, 1-录制声音,0-未录制',

    -- 调用asr识别
    vpr_id string comment '声纹id',
    question_text string comment '识别的文本,识别得文本为空也不会请求nlp',
    question_language string comment '识别的文本,语言类型',
    audio_length int comment '声音文件大小，字节',
    asr_vendor_delay int comment 'asr vendor 供应商延迟，毫秒',
    rcc_delay int comment 'rcc 延迟，毫秒',
    forward_delay int comment '转发延迟',
    asr_recognize_k8s_svc_name string comment 'k8s smart voice control 名称',
    asr_recognize_k8s_env_name string comment 'k8s环境名称',
    asr_recognize_event_time string comment '事件发生时时间',
    is_asr_recognize int comment '是否调用asr识别,1-调用,0-未调用',

    -- asr hit asr_domain
    asr_domain string comment 'asr domain',
    asr_third_total int comment '第三方asr识别时间,最后一个Pre END开始到返回最终文本结束',
    asr_start string comment 'asr识别开始时间',
    asr_end string comment 'asr识别完成时间',
    nlufwd string comment '转发到下一个应用,调用nlu或转发方式,空串 回显,reception,service app,nlu',
--
    wake_status int comment '-1:异常错误,0:休眠状态,1:正常，0 状态不精准,此状态下不会请求nlp,-1 可能会请求,1:唤醒,会请求nlp',
    wake_status_name string comment '-1:异常错误,0:休眠状态,1:正常，0 状态不精准,此状态下不会请求nlp,-1 可能会请求,1:唤醒,会请求nlp',
    nlp_total int comment 'asrctrl 请求nlp处理,nlp总耗时',
    gap int comment '音频上传结束到asr识别完成的时间',
    vocal_total int comment '声纹识别耗时',
    last_pre_end_start string comment '最后一次pre end开始时间',
    asrctrl_duration int comment '整个请求中asr 耗时时长,asr持续时间',
    x_b3_traceid string comment 'k8s中带的x-b3-traceid',
    asrctrl_inner int comment 'asrctrl 内部耗时 (去掉asr、nlu、转发答案等剩余的asrctrl耗时)',
    caller string comment '回调方法',
    is_pre_end int comment '是否有pre end',
    asrctrl_robot_skill_total int comment 'asrctrl调用robotskill的总耗时（包含了handleAction发送答案的耗时）',
    asr_nlp_total int comment 'ASR+NLP主要业务耗时',
    audio_size int comment '音频大小字节',
    asr_domain_k8s_svc_name string comment 'k8s smart voice control 名称',
    asr_domain_k8s_env_name string comment 'k8s环境名称',
    asr_domain_event_time string comment 'asr domain 发生时间',
    is_asr_domain int comment 'asr是否命中domain, 1-调用domain 2-未调用domain',

    -- asr 往 reception 发送识别文本
    is_hi_online int comment 'hi座席状态，1-在线，0-不在线',
    latitude string comment '纬度',
    longitude string comment '经度',
    txt_send_delay int comment '发送时长，毫秒',
    txt_queue_delay int comment '排队时长，毫秒',
    txt_total_delay int comment 'asr send 总时延，毫秒',
    asr_text_k8s_svc_name string comment 'k8s smart voice control 名称',
    asr_text_k8s_env_name string comment 'k8s环境名称',
    asr_text_event_time string comment '事件发生时时间',
    is_asr_text int comment '是否asr发送识别的文本, 1-asr发送文本 0-asr未发送文本',

    -- reception 往 robot skill 发送识别的文本
    reception_text_k8s_svc_name string comment 'k8s smart voice control 名称',
    reception_text_k8s_env_name string comment 'k8s环境名称',
    reception_text_event_time string comment '事件发生时时间',
    is_reception_text int comment 'reception是否发送文本到robot skill, 1-reception发送 0-reception未发送',

    -- robot 往 hari switch 发送识别文本
    harix_switch_id string comment 'hari switch的id',
    type_url string comment '请求的url类型',
    robot_text_msg_from string comment '消息发出的模块',
    robot_text_msg_send_name string comment '消息发出的用户名',
    robot_text_msg_id string comment '消息id',
    robot_text_msg_receive_name string comment '收消息的用户名',
    robot_text_msg_type string comment 'switch交换quest类型,消息的type',
    robot_text_msg_dest string comment 'switch交换的目的地,收消息的模块名',
    robot_text_k8s_svc_name string comment 'k8s smart voice control 名称',
    robot_text_k8s_env_name string comment 'k8s环境名称',
    robot_text_event_time string comment '事件发生时时间',
    is_robot_text int comment '机器人是否发送文本到hari switch, 1-robot skill发送文本 2-robot skill未发送文本',

    -- nlp 处理 识别的文本 返回意图
    sv_agent_id string comment 'smart voice 的agent id',
    sv_agent_name string comment 'smart voice 的agent名称',
    algo string comment '算法类型',
    cost int comment 'asr请求nlu处理消耗时延，毫秒',
    matched_template string comment'命中得模板',
    env_info string comment 'nlu环境信息',
    before_context string comment '之前请求的文本内容，环境上下文?',
    in_context string comment '当前请求的文本内容，环境上下文?',
    out_context string comment '输出上下文?',
    param_info string comment '请求的参数信息?',
    parameters string comment '请求的参数?',
    qa_result string comment 'qa 结果',
    question_group_id string comment '问题的分组id',
    sim_question_score decimal(18,16) comment '问题匹配相似度',
    answer_text string comment '识别的文本后返回的qa文本',
    answer_language string comment 'nlu处理识别的文本后返回的意图文本,语言类型',
    sv_answer_text string comment 'nlu处理识别的文本后返回的意图文本',
    session_id string comment 'session id',
    qa_from string comment '提供qa服务的模块',
    supplier string comment 'qa服务提供者',
    supplier_type string comment 'qa服务提供者类型',
    third_cost int comment '第三方服务耗时,毫秒',
    third_session_id string comment '第三方服务session id',
    nlu_k8s_svc_name string comment 'k8s smart voice control 名称',
    nlu_k8s_env_name string comment 'k8s环境名称',
    nlu_event_time string comment '事件发生时时间',
    is_nlp int comment '是否调用了nlp处理识别文本, 1-asr请求nlp处理 0-asr未请求nlp处理',


    -- asr 往reception 发送qa
    confidence double comment '内容置信度',
    qa_send_delay int comment '发送时长，毫秒',
    qa_queue_delay int comment '排队时长，毫秒',
    qa_total_delay int comment 'asr send 总时延，毫秒',
    asr_qa_msg_from string comment '消息来源，例如来源 SV',
    tts_step string comment '当前question的tts步骤号,使用1,2,3表示',
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
    asr_qa_k8s_svc_name string comment 'k8s smart voice control 名称',
    asr_qa_k8s_env_name string comment 'k8s环境名称',
    asr_qa_event_time string comment '事件发生时时间',
    is_asr_qa int comment 'asr是否发送了qa, 1-asr发送qa 0-asr未发送qa',

    -- reception 往 robot 发送qa
    reception_qa_k8s_svc_name string comment 'k8s smart voice control 名称',
    reception_qa_k8s_env_name string comment 'k8s环境名称',
    reception_qa_event_time string comment '事件发生时时间',
    is_reception_qa int comment 'reception是否发送qa, 1-reception发送qa 0-reception未发送qa',

    -- robot 往 hari 发送qa

    robot_qa_msg_from string comment '消息发出的模块',
    robot_qa_msg_send_name string comment '消息发出的用户名',
    robot_qa_msg_id string comment '消息id',
    robot_qa_msg_receive_name string comment '收消息的用户名',
    robot_qa_msg_type string comment 'switch交换quest类型,消息的type',
    robot_qa_msg_dest string comment 'switch交换的目的地,收消息的模块名',
    robot_qa_send_delay int comment 'robot send delay？',
    robot_qa_k8s_svc_name string comment 'k8s smart voice control 名称',
    robot_qa_k8s_env_name string comment 'k8s环境名称',
    robot_qa_event_time string comment '事件发生时时间',
    is_robot_qa int comment 'robot skill 是否发送qa, 1-robot skill发送qa 0-robot skill 未发送qa',

    -- qa 坐席增强
    hi_intelligence_msg string comment '数据内容,对应日志中eventContent,hi 事件中的消息',
    hi_intelligence_msg_from string comment '数据内容来源，hiChatListConfirmSimQ-确认问题 答案中的相似问题,hiChatListConfirmGW-确认问题  GW 中的问题,hiChatListConfirmAiAnswer-确认答案 有无信心都可点击,hiQuestionPanel - 输入框输入的问题,hiAnswerPanel - 输入框输入的答案,hiDancePanel - 常用舞蹈  对应上面isAction 为true,hiActionPanel - 常用动作  对应上面isAction 为true,hiShortcutPanel - 自定义面板中  自定义按钮，包括问题和答案,hiPhaseList - 常用短语',
    hi_intelligence_msg_from_name string comment '数据内容来源，hiChatListConfirmSimQ-确认相似问题,hiChatListConfirmGW-确认问题gateway问题,hiChatListConfirmAiAnswer-确认答案 ,hiQuestionPanel - 输入框输入问题,hiAnswerPanel - 输入框输入答案,hiDancePanel - 常用舞蹈  对应上面isAction 为true,hiActionPanel - 常用动作  对应上面isAction 为true,hiShortcutPanel - 自定义按钮，包括问题和答案,hiPhaseList - 常用短语',
    hi_intelligence_msg_type string comment 'question、answer、action',
    hi_intelligence_k8s_svc_name string comment 'k8s smart voice control 名称',
    hi_intelligence_k8s_env_name string comment 'k8s环境名称',
    hi_intelligence_event_time string comment 'hi 发生时间',
    is_hi_intelligence int comment '是否坐席增强, 1-坐席增强 0-坐席未增强',

    -- 坐席发送qa
    hi_qa_msg_from string comment '消息发出的模块',
    hi_qa_msg_send_name string comment '消息发出的用户名',
    hi_qa_msg_id string comment '消息id',
    hi_qa_msg_type string comment 'switch交换quest类型,消息的type',
    hi_qa_k8s_svc_name string comment 'k8s smart voice control 名称',
    hi_qa_k8s_env_name string comment 'k8s环境名称',
    hi_qa_event_time string comment '事件发生时时间',
    is_hi_qa int comment '坐席是否发送qa, 1-坐席发送qa 2-坐席未发送qa',
    conversation_start_time string comment '会话开始时间',
    conversation_end_time string comment '会话结束时间',
    cmd_k8s_env_name string comment '会话环境信息,如果环境为221,join时候使用232join'
)
comment 'audio全流程宽表'
partitioned by(dt string comment 'dt分区字段')
stored as parquet
location '/data/cdmdwm/dwm_cmd_audio_process_i_d';