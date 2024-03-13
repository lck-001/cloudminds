create external table cdmods.ods_bigdata_db_c0032_fsimage_i_d (
     owner string comment '创建者',
     path string comment 'hdfs路径',
     modifiedTime string comment '修改时间',
     isFile boolean comment 'true 文件 false目录',
     fileSize bigint comment '文件大小',
     createTime string comment '创建时间',
     department string comment '部门'
)
comment 'hdfsimage 的信息'
PARTITIONED BY (dt string COMMENT '日期')
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
STORED AS TEXTFILE;