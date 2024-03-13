-- cdmdq.crt_ods_data_check ods层数据校验
create external table cdmdq.ods_data_check(
    database_name string comment '数据库名',
    table_type string comment '表类型,增量表/全量表',
    table_type_name string comment '表类型,增量表/全量表',
    total_cnt int comment '全表数据量',
    nil_cnt int comment '表的空值数量',
    inc_cnt int comment '新增数量',
    dup_cnt int comment '重复数据数量,增量表,全量表重复数据量，快照表是当日快照中重复的量'
)
comment 'ods层数据校验'
partitioned by (dt string comment '校验数据所对应时间的日期',table_name string comment '表名')
stored as parquet
location '/data/cdmdq/ods_data_check';

-- 创建pg库
CREATE TABLE ods_data_check(
    id text not null,
    database_name text,
    table_name text,
    table_type text,
    table_type_name text,
    total_cnt int,
    nil_cnt int,
    inc_cnt int,
    dup_cnt int,
    dt text,
    PRIMARY KEY(id)
);
comment on table ods_data_check is 'ods数据校验';
comment on column ods_data_check.id is '主键ID，数据库表名环境日期组成的md5';
comment on column ods_data_check.database_name is '数据库名';
comment on column ods_data_check.table_name is '表名';
comment on column ods_data_check.table_type is '表类型';
comment on column ods_data_check.table_type_name is '表类型名称';
comment on column ods_data_check.total_cnt is '总数居量';
comment on column ods_data_check.nil_cnt is '空值量';
comment on column ods_data_check.inc_cnt is '新增数据量';
comment on column ods_data_check.dup_cnt is '重复量';
comment on column ods_data_check.dt is '校验日期';