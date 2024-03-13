-- cdmdq.crt_dwm_data_check dwm层数据校验
create external table cdmdq.dwm_data_check(
    database_name string comment '数据库名',
    table_type string comment '表类型,增量表/全量表',
    table_type_name string comment '表类型,增量表/全量表',
    dim_uncover string comment '维度未覆盖数',
    dist_uncover string comment '去重后未覆盖数',
--     ids_uncover string comment '未覆盖的id',
    inc_cnt int comment '新增数量'
)
comment 'dwm层数据校验'
partitioned by (dt string comment '校验数据所对应时间的日期',table_name string comment 'dwm层表名')
stored as parquet
location '/data/cdmdq/dwm_data_check';

CREATE TABLE dwm_data_check(
    id text not null,
    database_name text,
    table_name text,
    table_type text,
    table_type_name text,
    inc_cnt int,
    dim_uncover text,
    dist_uncover text,
--     ids_uncover text,
    dt text,
    PRIMARY KEY(id)
);
comment on table dwm_data_check is 'dwm数据校验';
comment on column dwm_data_check.id is '主键ID，数据库表名环境日期组成的md5';
comment on column dwm_data_check.database_name is '数据库名';
comment on column dwm_data_check.table_name is '表名';
comment on column dwm_data_check.table_type is '表类型';
comment on column dwm_data_check.table_type_name is '表类型名称';
comment on column dwm_data_check.inc_cnt is '新增数据量';
comment on column dwm_data_check.dim_uncover is '未覆盖维度的数量';
comment on column dwm_data_check.dist_uncover is '去重后未覆盖维度的数量';
-- comment on column dwm_data_check.ids_uncover is '未覆盖维度的id';
comment on column dwm_data_check.dt is '校验日期';