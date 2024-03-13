-- cdmods.ods_cms_db_c0017_faq_op_log_i_d
create external table cdmods.ods_cms_db_c0017_faq_op_log_i_d(
     faq_info string comment 'faq_info'
)
comment 'cms.faq_op_log'
PARTITIONED BY (dt string COMMENT '日期')
STORED AS TEXTFILE;