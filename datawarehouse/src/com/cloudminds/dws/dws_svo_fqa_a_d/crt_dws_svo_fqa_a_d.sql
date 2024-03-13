--  modify by dave.liang at 2021-09-26
create external table cdmdws.dws_svo_fqa_a_d
(
--     dim_svo_fqa_cate_sh_d
    fqa_cate__cate_name_path string comment '分类cate name path',
    fqa_cate__cate_id_path string comment '分类id path',
    fqa_cate__cate_name string comment '分类名字',
    fqa_cate__level int comment 'level',
--  dwd_cms_svo_fqa_i_d
    fqa__fqa_id string comment 'fqa  ID',
    fqa__fqa_cate_id string comment 'cate表id',
    fqa__title string comment '展示标题',
    fqa__is_work int comment '是否生效 1:no 2:yes',
    fqa__is_work_name string comment '是否生效 1:no 2:yes',
    fqa__create_time string comment '添加时间',
    fqa__event_time string comment '最后更新时间',
    fqa__push_action int comment '推送动作 为了推送给kafka 1:add 2:delete 3:update',
    fqa__push_action_name string comment  '推送动作 为了推送给kafka 1:add 2:delete 3:update',
    fqa__is_need_push int comment '是否需要推送kafka 1:yes 2:no',
    fqa__is_del int comment '是否删除 1:no 2:yes',
    fqa__question string comment '问题的json',
    fqa__answer string comment '回答的json',
    fqa__emoji string comment 'emoji 表情',
    fqa__last_seq_id string comment 'kafka seq_id',
    fqa__is_syn_status int comment '是否同步完成 对应last_seq_id 1:yes 2:no',
    fqa__audit_name string comment '记录操作轨迹',
    fqa__sv_msg string comment '记录同步轨迹',
    fqa__source string comment '来源',
    fqa__sys_cate_id string comment '系统级分类',
    fqa__sv_cate_id string comment 'sv types',
    fqa__op_db int comment 'db操作类型:INSERT:1;UPDATE:2;DELETE:3',
    fqa__k8s_env_name string comment 'k8s环境名称'

) comment ' fqa 全量表'
    stored as parquet ;