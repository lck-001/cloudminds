create external table cdmods.ods_bigdata_db_c0036_table_info_s_d (
     tbl_id bigint comment '表id',
     create_time int comment '创建时间',
     db_id bigint comment '数据库id',
     last_access_time int comment '最近访问时间',
     owner string comment '创建者',
     owner_type string comment '创建者类型',
     sd_id bigint comment 'SD_ID',
     tbl_name string comment '表名称',
     tbl_type string comment '表类型',
     view_expanded_text string comment '',
     view_original_text string comment '',
     db_name string comment '数据库名称',
     numRows string comment '从table里统计的总条数',
     partionNumRows string comment '从分区里统计的总条数',
     totalSize string comment '总数据大小',
     partionSize string comment '从分区里统计的总大小',
     last_modified_time string comment '最近修改时间',
     columns string comment '所有列名'
)
comment 'hive表的信息'
PARTITIONED BY (dt string COMMENT '日期')
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
STORED AS TEXTFILE;