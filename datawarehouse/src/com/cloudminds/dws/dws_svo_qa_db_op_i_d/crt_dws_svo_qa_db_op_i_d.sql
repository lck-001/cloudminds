--  modify by dave.liang at 2021-09-26
create external table cdmdws.dws_svo_qa_db_op_i_d
(
--     dim_svo_user_qa_cate_sh_d
    cate_name_path string comment '分类cate name path',
    cate_id_path string comment '分类id path',
    level int comment 'level',
    qa_type string comment 'qa 类型：common_sense_qa 、user_qa',
--      dwd_cms_svo_user_qa_i_d
    agent_id string comment 'sv agent id',
    is_del int comment '是否删除1:no 2:yes',
    op_db int comment 'db操作类型:INSERT:1;UPDATE:2;DELETE:3',
    cnt int comment  'qa操作统计数',
    cnt_cycle string comment '统计周期, day : 天, week: 周, month: 月',
    k8s_env_name string comment 'k8s环境名称'
) comment ' qa 数据库操作 每日增量表'
    partitioned by(dt string comment 'dt分区字段')
    stored as parquet ;