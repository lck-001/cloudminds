create external table cdmods.ods_crio_db_c0038_t_category_i_d (
     id bigint comment '分类ID',
     parent_id bigint comment '父分类ID，根节点的父分类ID为0',
     tenant_id int comment '租户ID',
     tenant_code string comment '租户编码',
     branch_id int comment '分支机构ID',
     biz_code string comment '字典对应的业务编码',
     name string comment '分类名称',
     sort int comment '排序，数值越小越靠前',
     layer int comment '树的深度（层数）',
     language int comment '多语言: 0-简体中文 1-English 2-繁體中文 3-日本語 4-ภาษาไทย',
     status string comment '状态 1：有效 9：删除',
     create_time bigint comment '创建时间',
     update_time bigint comment '修改时间',
     version int comment '乐观锁版本号',
     event_time bigint comment '事件时间',
     bigdata_method string comment 'db操作类型:r c u d',
     k8s_env_name string  comment '环境名称'
)
comment '目录表的信息'
PARTITIONED BY (dt string COMMENT '日期')
STORED AS parquet;