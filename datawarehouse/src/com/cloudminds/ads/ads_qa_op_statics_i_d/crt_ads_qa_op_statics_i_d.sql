create external table cdmads.ads_qa_op_statics_i_d (
    qa_id string comment 'qa的id',
    qa_type string comment 'qa类型,fqa user_qa',
    operator_id string comment '操作者id',
    operator string comment '操作者',
    agent_id string comment 'agent id,只有user_qa才有',
    qa_cate_id string comment 'qa类别id',
    cate_id_path string comment 'qa_cate分类id path',
    cate_name_path string comment 'qa_cate name path',
    work_cnt_d int comment '有效任务计数'
)
comment 'qa_op 操作记录的cube统计表'
partitioned by(dt string comment '分区日期')
stored as parquet
location '/data/cdmads/ads_qa_op_statics_i_d';