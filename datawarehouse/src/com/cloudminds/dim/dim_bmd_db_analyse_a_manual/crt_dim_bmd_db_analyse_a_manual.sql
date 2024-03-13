-- cdmdim.dim_bmd_db_analyse_a_manual 大数据表名与部门对应信息,手动维护
create external table cdmdim.dim_bmd_db_analyse_a_manual(
     db_type int comment '1:hive 2 clickhouse',
     name string comment '数据库名称/或者数据库加表名称',
     analyse_type int comment '1:部门 2:主题',
     analyse_value string comment 'analyse_type为1就填权限部门，为2填主题名'
)
comment '数据表名与部门对应信息'
row format delimited
fields terminated by ','
stored as textfile
location '/data/cdmdim/dim_bmd_db_analyse_a_manual';

--数据位置 /data/cdmdim/dim_bmd_db_analyse_a_manual/data.txt