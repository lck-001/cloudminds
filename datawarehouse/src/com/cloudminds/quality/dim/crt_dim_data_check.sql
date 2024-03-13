-- cdmdq.crt_dim_data_check dim层数据校验
create external table cdmdq.dim_data_check(
    database_name string comment '数据库名',
    table_type string comment '表类型,增量表/全量表',
    table_type_name string comment '表类型,增量表/全量表',
    inc_cnt int comment '新增数量',
    total_cnt int comment 'dim层数据数量',
    nil_cnt int comment 'dim层对应环境下的空值数,null 或者 空串,sh表为最新状态下的null 或者 空串',
    dup_cnt int comment '重复数据数量或者是快照表时间段重叠'
)
comment 'dim层数据校验'
partitioned by (dt string comment '校验数据所对应时间的日期',table_name string comment 'dim层表名',k8s_env_name string comment 'dim环境下')
stored as parquet
location '/data/cdmdq/dim_data_check';


-- pg建表
CREATE TABLE dim_data_check(
    id text not null,
    database_name text,
    table_name text,
    table_type text,
    table_type_name text,
    k8s_env_name text,
    inc_cnt int,
    total_cnt int,
    nil_cnt int,
    dup_cnt int,
    dt text,
    PRIMARY KEY(id)
);
comment on table dim_data_check is 'dim数据校验';
comment on column dim_data_check.id is '主键ID，数据库表名环境日期组成的md5';
comment on column dim_data_check.database_name is '数据库名';
comment on column dim_data_check.table_name is '表名';
comment on column dim_data_check.table_type is '表类型';
comment on column dim_data_check.table_type_name is '表类型名称';
comment on column dim_data_check.k8s_env_name is 'dim数据环境';
comment on column dim_data_check.inc_cnt is '新增数据量';
comment on column dim_data_check.total_cnt is '总数居量';
comment on column dim_data_check.nil_cnt is '空值量';
comment on column dim_data_check.dup_cnt is '重复量';
comment on column dim_data_check.dt is '校验日期';