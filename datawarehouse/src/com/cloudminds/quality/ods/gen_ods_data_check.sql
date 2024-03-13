--a
set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;

insert overwrite table cdmdq.ods_data_check partition(dt,table_name)
select
    'cdmods' as database_name,
    split('$table_name','_')[size(split('$table_name','_'))-2] as table_type,
    case when split('$table_name','_')[size(split('$table_name','_'))-2] = 'i' then '增量'
    when split('$table_name','_')[size(split('$table_name','_'))-2] = 's' then '快照'
    when split('$table_name','_')[size(split('$table_name','_'))-2] = 'a' then '全量'
    when split('$table_name','_')[size(split('$table_name','_'))-2] = 'sh' then '拉链'
    else '未知'
end as table_type_name,
nvl(t2.total_cnt,0) as total_cnt,
nvl(t3.nil_cnt,0) as nil_cnt,
nvl(t1.inc_cnt,0) as inc_cnt,
nvl(t4.dup_cnt,0) as dup_cnt,
'${e_dt_var}' as dt,
'$table_name' as table_name
from (
    select count(1) as inc_cnt,'$table_name' as col from cdmods.$table_name
) t1
left join (
    select count(1) as total_cnt,'$table_name' as col from cdmods.$table_name
) t2 on t1.col = t2.col
left join (
    select count(1) as nil_cnt,'$table_name' as col from cdmods.$table_name $null_where_sql_str
) t3 on t1.col = t3.col
left join (
    select count(1) as dup_cnt,'$table_name' as col from (select $null_columns from cdmods.$table_name group by $null_columns having count(1) >1 ) as tmp
) t4 on t1.col = t4.col;

select md5(concat('cdmods','$table_name','${e_dt_var}')) as id,* from cdmdq.ods_data_check where dt = '${e_dt_var}' and table_name = '$table_name'; --update_into_pgsql:postgres:cloud1688:172.16.31.1:32086:cdmdq:ods_data_check


--i
set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;

insert overwrite table cdmdq.ods_data_check partition(dt,table_name)
select
    'cdmods' as database_name,
    split('$table_name','_')[size(split('$table_name','_'))-2] as table_type,
    case when split('$table_name','_')[size(split('$table_name','_'))-2] = 'i' then '增量'
    when split('$table_name','_')[size(split('$table_name','_'))-2] = 's' then '快照'
    when split('$table_name','_')[size(split('$table_name','_'))-2] = 'a' then '全量'
    when split('$table_name','_')[size(split('$table_name','_'))-2] = 'sh' then '拉链'
    else '未知'
end as table_type_name,
nvl(t2.total_cnt,0) as total_cnt,
nvl(t3.nil_cnt,0) as nil_cnt,
nvl(t1.inc_cnt,0) as inc_cnt,
nvl(t4.dup_cnt,0) as dup_cnt,
'${e_dt_var}' as dt,
'$table_name' as table_name
from (
    select count(1) as inc_cnt,'$table_name' as col from cdmods.$table_name where dt ='${e_dt_var}'
) t1
left join (
    select count(1) as total_cnt,'$table_name' as col from cdmods.$table_name
) t2 on t1.col = t2.col
left join (
    select count(1) as nil_cnt,'$table_name' as col from cdmods.$table_name $null_where_sql_str
) t3 on t1.col = t3.col
left join (
    select count(1) as dup_cnt,'$table_name' as col from (select $null_columns from cdmods.$table_name group by $null_columns having count(1) >1 ) as tmp
) t4 on t1.col = t4.col;

select md5(concat('cdmods','$table_name','${e_dt_var}')) as id,* from cdmdq.ods_data_check where dt = '${e_dt_var}' and table_name = '$table_name'; --update_into_pgsql:postgres:cloud1688:172.16.31.1:32086:cdmdq:ods_data_check


--s
set spark.sql.crossJoin.enabled=true;
set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;

insert overwrite table cdmdq.ods_data_check partition(dt,table_name)
select
    'cdmods' as database_name,
    split('$table_name','_')[size(split('$table_name','_'))-2] as table_type,
    case when split('$table_name','_')[size(split('$table_name','_'))-2] = 'i' then '增量'
    when split('$table_name','_')[size(split('$table_name','_'))-2] = 's' then '快照'
    when split('$table_name','_')[size(split('$table_name','_'))-2] = 'a' then '全量'
    when split('$table_name','_')[size(split('$table_name','_'))-2] = 'sh' then '拉链'
    else '未知'
end as table_type_name,
nvl(t2.total_cnt,0) as total_cnt,
nvl(t3.nil_cnt,0) as nil_cnt,
nvl(t1.inc_cnt,0) as inc_cnt,
nvl(t4.dup_cnt,0) as dup_cnt,
'${e_dt_var}' as dt,
'$table_name' as table_name
from (
    SELECT (t1.cnt - t2.cnt) as inc_cnt,'$table_name' as col from ( SELECT 't' as t,count(1) as cnt from cdmods.$table_name where dt ='${e_dt_var}') t1 left join (SELECT 't' as t,count(1) as cnt from cdmods.$table_name where dt =date_sub('${e_dt_var}',1)) t2 on t1.t = t2.t
) t1
left join (
    select count(1) as total_cnt,'$table_name' as col from cdmods.$table_name where dt ='${e_dt_var}'
) t2 on t1.col = t2.col
left join (
    select count(1) as nil_cnt,'$table_name' as col from cdmods.$table_name $null_where_sql_str and dt ='${e_dt_var}'
) t3 on t1.col = t3.col
left join (
    select count(1) as dup_cnt,'$table_name' as col from (select $null_columns from cdmods.$table_name where dt ='${e_dt_var}' group by $null_columns having count(1) >1 ) as tmp
) t4 on t1.col = t4.col;

select md5(concat('cdmods','$table_name','${e_dt_var}')) as id,* from cdmdq.ods_data_check where dt = '${e_dt_var}' and table_name = '$table_name'; --update_into_pgsql:postgres:cloud1688:172.16.31.1:32086:cdmdq:ods_data_check
