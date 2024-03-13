-- cdmdwd.dwd_cms_svo_fqa_op_log_a_d faq_op操作记录
create external table cdmdwd.dwd_cms_svo_fqa_op_log_i_d (
    event_id string comment 'ID',
    fqa_id string comment 'qa的id',
    operator_id string comment '操作者id',
    operator string comment '操作者',
    operation string comment '操作动作',
    question_added string comment '增加的问题',
    question_add_num int comment '增加的问题数量',
    answer_added string comment '增加的回答',
    answer_add_num int comment '增加的回答数量',
    fqa_id_old string comment '旧的fqa_id,修改之前的fqa_id',
    fqa_cate_id_old string comment '旧的fqa_cate_id,修改之前的fqa_cate_id',
    fqa_cate_name_old string comment '旧的fqa_cate_name_id,修改之前的fqa_cate_name_id',
    status_old int comment '老的状态值：0没有生效 1生效',
    status_name_old string comment '老的状态值：0没有生效 1生效',
    emoji_old string comment '旧的emoji表情',
    tags_old string comment '旧的标签',
    question_sim_old string comment '旧的相似问题',
    answer_sim_old string comment '旧的相似回答',
    fqa_id_new string comment '新的的fqa_id,修改之后的fqa_id',
    fqa_cate_id_new string comment '新的fqa类别id',
    fqa_cate_name_new string comment '新的fqa类别名称',
    status_new int comment '新的状态值：0没有生效 1生效',
    status_name_new string comment '新的状态值：0没有生效 1生效',
    emoji_new string comment '新的emoji表情',
    tags_new string comment '新的标签',
    question_sim_new string comment '新的相似问题',
    answer_sim_new string comment '新的相似回答',
    file_path string comment '文件路径',
    memo string comment '备忘录',
    language string comment '语言',
    k8s_env_name string comment '数据来源环境',
    event_time string comment '操作时间',
    ext string comment '原始数据'
)
comment 'fqa_op_log记录'
partitioned by(dt string comment 'dt分区字段')
stored as parquet
location '/data/cdmdwd/dwd_cms_svo_fqa_op_log_i_d';