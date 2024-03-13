-- cdmods.ods_cms_db_c0016_fqacate_s_d
create external table cdmods.ods_cms_db_c0016_fqacate_s_d(
     id int comment 'ID',
     pid int comment '父id',
     cate_name string comment '分类名字',
     level int comment 'level',
     sort int comment '排序 越小越靠前',
     status string comment '是否生效 no yes',
     add_time string comment '添加时间',
     update_time string comment '最后更新时间',
     push_action string comment '推送动作 为了推送给kafka add delete update',
     need_push string comment '是否需要推送kafka no yes',
     is_del string comment '是否删除 no yes',
     last_seq_id string comment 'kafka seq_id',
     synstatus string comment '是否同步完成 yes no',
     auditname string comment '记录操作轨迹',
     svmsg string comment '记录同步轨迹',
     source string comment '来源',
     k8s_env_name string comment '环境名称'
)
comment 'kbs_cms.fqacate'
PARTITIONED BY (dt string COMMENT '日期')
STORED AS parquet;