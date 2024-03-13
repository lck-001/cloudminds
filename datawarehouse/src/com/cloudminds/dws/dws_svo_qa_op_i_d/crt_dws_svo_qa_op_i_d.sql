-- cdmdws.dws_svo_qa_op_i_d qa_op每个operator每天每个qa操作轻度统计
create external table cdmdws.dws_svo_qa_op_i_d (
    qa_id string comment 'qa的id',
    qa_type string comment 'qa类型,fqa user_qa',
    operator_id string comment '操作者id',
    operator string comment '操作者',
    operation string comment '操作动作',
    agent_id string comment 'agent id,只有user_qa才有',
    qa_id_old string comment '旧的qa id',
    qa_cate_id_old string comment '旧的fqa类别id',
    qa_cate_name_old string comment '旧的fqa类别名称',
    emoji_old string comment '旧的emoji表情',
    tags_old string comment '旧的标签',
    pid_old string comment '旧的cate父id',
    level_old int comment '旧的cate level',
    cate_id_path_old string comment '旧的cate分类id path',
    cate_name_path_old string comment '分类cate name path',
    qa_id_new string comment '新的qa id',
    qa_cate_id_new string comment '新的fqa类别id',
    qa_cate_name_new string comment '新的fqa类别名称',
    emoji_new string comment '新的emoji表情',
    tags_new string comment '新的标签',
    pid_new string comment '新的cate父id',
    level_new int comment '新的cate level',
    cate_id_path_new string comment '新的cate 分类id path',
    cate_name_path_new string comment '分类cate name path',
    work_cnt_d int comment '当前qa的每人每天的work数',
    file_path string comment '文件路径',
    memo string comment '备忘录',
    language string comment '语言'
)
comment 'qa_op每个operator每天每个qa操作轻度统计'
partitioned by(dt string comment 'dt分区字段')
stored as parquet
location '/data/cdmdws/dws_svo_qa_op_i_d';