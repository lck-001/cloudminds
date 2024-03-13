--  modify by dave.liang at 2022-08-10
create external table cdmdws.dws_svo_qa_stat_i_d
(
    qa_type string comment 'qa 类型：common_sense_qa 、user_qa',
--     dim_svo_user_qa_cate_sh_d
    cate_name_path string comment '分类cate name path',
    cate_name string comment '分类cate name',
    cate_level int comment 'level',
--      dwd_cms_svo_user_qa_i_d
    agent_id string comment 'sv agent id',

    work_total int comment  'qa生效总数',
    total string comment 'qa——总数',
    k8s_env_name string comment 'k8s环境名称'
) comment ' qa 数据库操作 每日统计QA总量 （增量表）'
    partitioned by(dt string comment 'dt分区字段,事件采集的日期')
    stored as parquet ;