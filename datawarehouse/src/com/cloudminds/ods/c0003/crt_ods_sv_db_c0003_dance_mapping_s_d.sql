-- cdmods.ods_sv_db_c0003_dance_mapping_s_d dance_mapping 映射表 需要intent_label和机器人类型才能确定
create external table cdmods.ods_sv_db_c0003_dance_mapping_s_d (
    id bigint comment 'dance mapping 自增id',
    intent_label string comment '意图标签名',
    algo_intent string comment '算法意图',
    robot_type string comment '机器人类型',
    robot_intent string comment '机器人意图',
    created_time string comment '意图创建时间',
    updated_time string comment '意图更新时间'
)
comment 'sv库action表 semantic.dance_mapping,需要intent_label和robot_type才能确定'
PARTITIONED BY (dt string comment '日期')
STORED AS parquet
location '/data/cdmods/ods_sv_db_c0003_dance_mapping_s_d';