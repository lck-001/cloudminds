-- cdmods.ods_cms_db_c0017_qa_op_log_i_d
create external table cdmods.ods_cms_db_c0017_qa_op_log_i_d(
     qa_info string comment 'qa_info'
)
comment 'cms.qa_op_log'
PARTITIONED BY (dt string COMMENT '日期')
STORED AS TEXTFILE;