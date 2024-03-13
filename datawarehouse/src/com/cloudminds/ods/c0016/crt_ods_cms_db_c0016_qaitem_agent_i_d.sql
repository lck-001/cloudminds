-- cdmods.ods_cms_db_c0016_qaitem_agent_i_d
create external table cdmods.ods_cms_db_c0016_qaitem_agent_i_d(
     id int comment 'ID',
     uuid string comment 'uuid',
     sys_cate_id int comment '系统级分类',
     sv_cate_id string comment 'sv types',
     cate_id int comment 'cate表id',
     agent_id string comment 'sv agent id',
     agent_secret string comment 'sv agent secret',
     title string comment '展示标题',
     status int comment '是否生效 1:no 2:yes',
     add_time string comment '添加时间',
     update_time string comment '最后更新时间',
     push_action int comment '推送动作 为了推送给kafka 1:add 2:delete 3:update',
     need_push int comment '是否需要推送kafka 1:yes 2:no',
     is_del int comment '是否删除1:no 2:yes',
     question string comment '问题的json',
     answer string comment '回答的json',
     emoji string comment '',
     synstatus int comment '是否同步完成 对应last_seq_id',
     auditname string comment '记录操作轨迹',
     svmsg string comment '记录同步轨迹',
     news_id int comment '媒体资源id',
     news_url string comment '媒体资源url',
     payload string comment 'sv payload',
     source string comment '来源',
     tags string comment 'tags',
     bigdata_method string comment 'db操作类型:INSERT;UPDATE;DELETE'
)
comment 'kbs_cms.qaitem_agent'
PARTITIONED BY (dt string COMMENT '日期')
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
STORED AS TEXTFILE;