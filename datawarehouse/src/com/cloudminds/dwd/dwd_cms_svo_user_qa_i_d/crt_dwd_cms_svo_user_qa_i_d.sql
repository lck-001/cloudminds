--  modify by dave.liang at 2021-09-17
create external table cdmdwd.dwd_cms_svo_user_qa_i_d (
                                                         user_qa_id string comment 'user qa ID',
                                                         uuid string comment 'uuid# 在es 端使用',
                                                         sys_cate_id string comment '系统级分类',
                                                         sv_cate_id string comment 'sv types',
                                                         user_qa_cate_id string comment 'cate表id',
                                                         agent_id string comment 'sv agent id',
                                                         agent_secret string comment 'sv agent secret',
                                                         title string comment '展示标题',
                                                         is_work int comment '是否生效 0:no 1:yes',
                                                         is_work_name string comment '是否生效 0:no 1:yes',
                                                         create_time string comment '添加时间',
                                                         event_time string comment '最后更新时间,事件发生时间',
                                                         push_action int comment '推送动作 为了推送给kafka 1:add 2:delete 3:update',
                                                         push_action_name int comment '推送动作 为了推送给kafka 1:add 2:delete 3:update',
                                                         is_need_push int comment '是否需要推送kafka 1:yes 0:no',
                                                         is_del int comment '是否删除0:no 1:yes',
                                                         question string comment '问题的json',
                                                         answer string comment '回答的json',
                                                         emoji string comment '',
                                                         is_syn_status int comment '是否同步完成 对应last_seq_id',
                                                         audit_name string comment '记录操作轨迹',
                                                         sv_msg string comment '记录同步轨迹',
                                                         news_id string comment '媒体资源id',
                                                         news_url string comment '媒体资源url',
                                                         payload string comment 'sv payload',
                                                         source string comment '来源',
                                                         tags string comment 'tags',
                                                         op_db int comment 'db操作类型:INSERT:1;UPDATE:2;DELETE:3',
                                                         k8s_env_name string comment 'k8s环境名称'
)
    comment 'cms 端用户QA库'
    partitioned by (dt string comment '分区日期')
    stored as parquet;