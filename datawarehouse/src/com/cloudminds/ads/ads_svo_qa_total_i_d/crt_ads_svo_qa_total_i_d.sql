create external table cdmads.ads_svo_qa_total_i_d (
    qa_type string comment 'qa类型,fqa user_qa',
    qa_cate_name_path string comment '分类cate name path',
    qa_title string comment '展示标题',
    qa_is_work int comment '是否生效 1:no 2:yes',
    qa_is_work_name string comment '是否生效 1:no 2:yes',
    qa_is_del int comment '是否删除 1:no 2:yes',
    qa_source string comment '来源',
    qa_sv_cate_id string comment 'sv types',
    k8s_env_name string comment 'k8s环境名称',
    total_cnt int comment 'qa总量',
    sv_agent_id string comment 'sv agent id'
)
comment 'qa总量统计汇总表'
partitioned by (dt string comment '分区日期')
stored as parquet;