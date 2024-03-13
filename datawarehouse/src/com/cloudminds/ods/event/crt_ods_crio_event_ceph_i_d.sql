---create by tq----
CREATE EXTERNAL TABLE cdmods.ods_crio_event_ceph_i_d(
    json_extract string
    )
comment '目录表的信息'
PARTITIONED BY (dt string COMMENT '日期')
STORED as textfile;