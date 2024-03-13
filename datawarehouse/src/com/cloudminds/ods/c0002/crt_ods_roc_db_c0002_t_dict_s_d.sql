-- cdmods.ods_roc_db_c0002_t_dict_s_d roc.t_dist robot类型
create external table cdmods.ods_roc_db_c0002_t_dict_s_d(
    id string comment '字典信息ID',
    type string  comment '字典分类',
    label string  comment '显示',
    value string  comment '值',
    value_type int comment '值的类型：0-数值，1-字符串,DEFAULT 0',
    i18n_label int comment '显示国际化：0-不需要国际化，1-需要国际化,DEFAULT 0',
    parent_id string comment '父字典信息ID',
    status int comment '状态: -1-删除, 0-正常,DEFAULT 0',
    create_time string comment '创建时间',
    update_time string comment '修改时间',
    description string comment '描述',
    display_order bigint comment '排序',
    i18n string comment '国际化',
    built_in int comment '是否内置: 0-否,1-是,DEFAULT 1',
    k8s_env_name string COMMENT '环境名'
)
comment 'roc系统字典表roc.t_dist robot类型'
PARTITIONED BY (dt string COMMENT '日期')
STORED AS parquet
location '/data/cdmods/ods_roc_db_c0002_t_dict_s_d';