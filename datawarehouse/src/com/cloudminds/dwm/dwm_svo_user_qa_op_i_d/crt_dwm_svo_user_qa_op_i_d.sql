--  modify by dave.liang at 2021-09-24
create external table cdmdwm.dwm_svo_user_qa_op_i_d
(
--     dim_svo_user_qa_cate_sh_d
    user_qa_cate__cate_name_path string comment '分类cate name path',
    user_qa_cate__cate_id_path string comment '分类id path',
    user_qa_cate__cate_name string comment '分类名字',
    user_qa_cate__level int comment 'level',
--      dwd_cms_svo_user_qa_i_d
    user_qa__user_qa_id string comment 'user qa ID',
    user_qa__uuid string comment 'uuid# 在es 端使用',
    user_qa__sys_cate_id string comment '系统级分类',
    user_qa__sv_cate_id string comment 'sv types',
    user_qa__user_qa_cate_id string comment 'cate表id',
    user_qa__agent_id string comment 'sv agent id',
    user_qa__agent_secret string comment 'sv agent secret',
    user_qa__title string comment '展示标题',
    user_qa__is_work int comment '是否生效 1:no 2:yes',
    user_qa__is_work_name string comment '是否生效 1:no 2:yes',
    user_qa__create_time string comment '添加时间',
    user_qa__event_time string comment '最后更新时间,事件发生时间',
    user_qa__push_action int comment '推送动作 为了推送给kafka 1:add 2:delete 3:update',
    user_qa__push_action_name int comment '推送动作 为了推送给kafka 1:add 2:delete 3:update',
    user_qa__is_need_push int comment '是否需要推送kafka 1:yes 2:no',
    user_qa__is_del int comment '是否删除1:no 2:yes',
    user_qa__question string comment '问题的json',
    user_qa__answer string comment '回答的json',
    user_qa__emoji string comment '',
    user_qa__is_syn_status int comment '是否同步完成 对应last_seq_id',
    user_qa__audit_name string comment '记录操作轨迹',
    user_qa__sv_msg string comment '记录同步轨迹',
    user_qa__news_id string comment '媒体资源id',
    user_qa__news_url string comment '媒体资源url',
    user_qa__payload string comment 'sv payload',
    user_qa__source string comment '来源',
    user_qa__tags string comment 'tags',
    user_qa__op_db int comment 'db操作类型:INSERT:1;UPDATE:2;DELETE:3',
    user_qa__k8s_env_name string comment 'k8s环境名称'
)comment 'user qa 操作事件表'
    partitioned by(dt string comment 'dt分区字段')
    stored as parquet ;