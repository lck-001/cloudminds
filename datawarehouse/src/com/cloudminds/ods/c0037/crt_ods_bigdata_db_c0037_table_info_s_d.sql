create external table cdmods.ods_bigdata_db_c0037_table_info_s_d (
     `database` string comment '数据库',
     name string comment '表名',
     engine string comment '引擎',
     is_temporary int comment '是否临时表',
     metadata_path string comment '文件位置',
     metadata_modification_time bigint comment '最近修改时间',
     create_table_query string comment '创建语句',
     engine_full string comment '引擎',
     partition_key string comment '分区key',
     sorting_key string comment '排序key',
     primary_key string comment '主键',
     sampling_key string comment '抽样key',
     storage_policy string comment '存储策略',
     total_rows bigint comment '总行数',
     total_bytes bigint comment '总存储量',
     lifetime_rows bigint comment 'lifetime行数',
     lifetime_bytes bigint comment 'lifetime存储量',
     `comment` string comment '备注',
     cloumns string comment '所有列名'
)
comment 'clickhouse表的信息'
PARTITIONED BY (dt string COMMENT '日期')
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
STORED AS TEXTFILE;