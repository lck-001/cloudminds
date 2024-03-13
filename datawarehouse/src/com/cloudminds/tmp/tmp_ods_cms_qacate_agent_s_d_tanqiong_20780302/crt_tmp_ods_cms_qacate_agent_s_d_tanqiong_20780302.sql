-- cdmtmp.tmp_ods_cms_qacate_agent_s_d_tanqiong_20780302
create external table cdmtmp.tmp_ods_cms_qacate_agent_s_d_tanqiong_20780302(
     id int comment 'ID',
     pid int comment '父id',
     cate_name string comment '分类名字',
     level int comment 'level',
     sort int comment '排序 越小越靠前',
     status string comment '状态 no yes',
     add_time string comment '添加时间',
     update_time string comment '最后更新时间',
     auditname string comment '记录操作轨迹',
     agent_id string comment 'sv agent id',
     source string comment '来源',
     uuid string comment 'uuid',
     memo string comment '',
     pub_id int comment '',
     k8s_env_name string comment '环境名称'
)
comment 'kbs_cms.qacate_agent通过实时表生成的快照表'
PARTITIONED BY (dt string COMMENT '日期')
STORED AS parquet;