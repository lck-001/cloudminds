-- a
set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;

insert overwrite table cdmdq.dim_data_check partition(dt,table_name,k8s_env_name)
select
    'cdmdim' as database_name,
    split('$table_name','_')[size(split('$table_name','_'))-2] as table_type,
    case when split('$table_name','_')[size(split('$table_name','_'))-2] = 'i' then '增量'
    when split('$table_name','_')[size(split('$table_name','_'))-2] = 's' then '快照'
    when split('$table_name','_')[size(split('$table_name','_'))-2] = 'a' then '全量'
    when split('$table_name','_')[size(split('$table_name','_'))-2] = 'sh' then '拉链'
    else '未知'
end as table_type_name,
nvl(t1.inc_cnt,0) as inc_cnt,
nvl(t2.total_cnt,0) as total_cnt,
nvl(t3.nil_cnt,0) as nil_cnt,
nvl(t4.dup_cnt,0) as dup_cnt,
'${e_dt_var}' as dt,
'$table_name' as table_name,
t.k8s_env_name
from
(select k8s_env_name from cdmdim.$table_name group by k8s_env_name) t
left join (
select k8s_env_name,count(1) as inc_cnt from cdmdim.$table_name group by k8s_env_name
) t1 on t.k8s_env_name = t1.k8s_env_name
left join (
select k8s_env_name,count(1) as total_cnt from cdmdim.$table_name group by k8s_env_name
) t2 on t.k8s_env_name = t2.k8s_env_name
left join (
select k8s_env_name,count(1) as nil_cnt from cdmdim.$table_name $null_where_sql_str group by k8s_env_name
) t3 on t.k8s_env_name = t3.k8s_env_name
left join (
select k8s_env_name,count(1) as dup_cnt from (select $null_columns,k8s_env_name from cdmdim.$table_name group by $null_columns, k8s_env_name having count(1) >1 ) as tmp group by k8s_env_name
) t4 on t.k8s_env_name = t4.k8s_env_name;


select md5(concat('cdmdim','$table_name','${e_dt_var}',k8s_env_name)) as id,* from cdmdq.dim_data_check where dt = '${e_dt_var}' and table_name = '$table_name'; --update_into_pgsql:postgres:cloud1688:172.16.31.1:32086:cdmdq:dim_data_check


-- i
set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;

insert overwrite table cdmdq.dim_data_check partition(dt,table_name,k8s_env_name)
select
    'cdmdim' as database_name,
    split('$table_name','_')[size(split('$table_name','_'))-2] as table_type,
    case when split('$table_name','_')[size(split('$table_name','_'))-2] = 'i' then '增量'
    when split('$table_name','_')[size(split('$table_name','_'))-2] = 's' then '快照'
    when split('$table_name','_')[size(split('$table_name','_'))-2] = 'a' then '全量'
    when split('$table_name','_')[size(split('$table_name','_'))-2] = 'sh' then '拉链'
    else '未知'
end as table_type_name,
nvl(t1.inc_cnt,0) as inc_cnt,
nvl(t2.total_cnt,0) as total_cnt,
nvl(t3.nil_cnt,0) as nil_cnt,
nvl(t4.dup_cnt,0) as dup_cnt,
'${e_dt_var}' as dt,
'$table_name' as table_name,
t.k8s_env_name
from
(select k8s_env_name from cdmdim.$table_name group by k8s_env_name) t
left join (
select k8s_env_name,count(1) as inc_cnt from cdmdim.$table_name where dt ='${e_dt_var}' group by k8s_env_name
) t1 on t.k8s_env_name = t1.k8s_env_name
left join (
select k8s_env_name,count(1) as total_cnt from cdmdim.$table_name group by k8s_env_name
) t2 on t.k8s_env_name = t2.k8s_env_name
left join (
select k8s_env_name,count(1) as nil_cnt from cdmdim.$table_name $null_where_sql_str group by k8s_env_name
) t3 on t.k8s_env_name = t3.k8s_env_name
left join (
select k8s_env_name,count(1) as dup_cnt from (select $null_columns,k8s_env_name from cdmdim.$table_name group by $null_columns, k8s_env_name having count(1) >1 ) as tmp group by k8s_env_name
) t4 on t.k8s_env_name = t4.k8s_env_name;


select md5(concat('cdmdim','$table_name','${e_dt_var}',k8s_env_name)) as id,* from cdmdq.dim_data_check where dt = '${e_dt_var}' and table_name = '$table_name'; --update_into_pgsql:postgres:cloud1688:172.16.31.1:32086:cdmdq:dim_data_check


-- sh
set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;

insert overwrite table cdmdq.dim_data_check partition(dt,table_name,k8s_env_name)
select
    'cdmdim' as database_name,
    split('$table_name','_')[size(split('$table_name','_'))-2] as table_type,
    case when split('$table_name','_')[size(split('$table_name','_'))-2] = 'i' then '增量'
    when split('$table_name','_')[size(split('$table_name','_'))-2] = 's' then '快照'
    when split('$table_name','_')[size(split('$table_name','_'))-2] = 'a' then '全量'
    when split('$table_name','_')[size(split('$table_name','_'))-2] = 'sh' then '拉链'
    else '未知'
end as table_type_name,
nvl(t1.inc_cnt,0) as inc_cnt,
nvl(t2.total_cnt,0) as total_cnt,
nvl(t3.nil_cnt,0) as nil_cnt,
nvl(t4.dup_cnt,0) as dup_cnt,
'${e_dt_var}' as dt,
'$table_name' as table_name,
t.k8s_env_name
from
(select k8s_env_name from cdmdim.$table_name where dt ='${e_dt_var}' group by k8s_env_name) t
left join (
    select
    x1.k8s_env_name,
    (cnt1-cnt2) as inc_cnt
    from (
        select k8s_env_name,count(1) as cnt1 from cdmdim.$table_name where dt ='${e_dt_var}' group by k8s_env_name
    ) x1 left join (
            select k8s_env_name,count(1) as cnt2 from cdmdim.$table_name where dt =date_sub('${e_dt_var}',1) group by k8s_env_name
    ) x2 on x1.k8s_env_name = x2.k8s_env_name
) t1 on t.k8s_env_name = t1.k8s_env_name
left join (
    select k8s_env_name,count(1) as total_cnt from cdmdim.$table_name where dt ='${e_dt_var}' group by k8s_env_name
) t2 on t.k8s_env_name = t2.k8s_env_name
left join (
    select k8s_env_name,count(1) as nil_cnt from (
        select * from cdmdim.$table_name where end_time = '9999-12-31 23:59:59.999'
    ) p $null_where_sql_str group by k8s_env_name
) t3 on t.k8s_env_name = t3.k8s_env_name
left join (
select
k8s_env_name,
count(1) as dup_cnt
from (
    select
    end_time,
    k8s_env_name,
    lag(end_time,1,'') over( partition by $null_columns,k8s_env_name order by end_time ) as tmp_end_time
    from cdmdim.$table_name where dt ='${e_dt_var}'
) p1 where tmp_end_time >= start_time group by k8s_env_name
) t4 on t.k8s_env_name = t4.k8s_env_name;


select md5(concat('cdmdim','$table_name','${e_dt_var}',k8s_env_name)) as id,* from cdmdq.dim_data_check where dt = '${e_dt_var}' and table_name = '$table_name'; --update_into_pgsql:postgres:cloud1688:172.16.31.1:32086:cdmdq:dim_data_check



-- s

set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;

insert overwrite table cdmdq.dim_data_check partition(dt,table_name,k8s_env_name)
select
    'cdmdim' as database_name,
    split('$table_name','_')[size(split('$table_name','_'))-2] as table_type,
    case when split('$table_name','_')[size(split('$table_name','_'))-2] = 'i' then '增量'
    when split('$table_name','_')[size(split('$table_name','_'))-2] = 's' then '快照'
    when split('$table_name','_')[size(split('$table_name','_'))-2] = 'a' then '全量'
    when split('$table_name','_')[size(split('$table_name','_'))-2] = 'sh' then '拉链'
    else '未知'
end as table_type_name,
nvl(t1.inc_cnt,0) as inc_cnt,
nvl(t2.total_cnt,0) as total_cnt,
nvl(t3.nil_cnt,0) as nil_cnt,
nvl(t4.dup_cnt,0) as dup_cnt,
'${e_dt_var}' as dt,
'$table_name' as table_name,
t.k8s_env_name
from
(select k8s_env_name from cdmdim.$table_name group by k8s_env_name) t
left join (
SELECT t1.k8s_env_name,(t1.cnt - t2.cnt) as inc_cnt from ( SELECT k8s_env_name,count(1) as cnt from cdmdim.$table_name where dt ='${e_dt_var}' group by k8s_env_name) t1 left join (SELECT k8s_env_name,count(1) as cnt from cdmdim.$table_name where dt =date_sub('${e_dt_var}',1) group by k8s_env_name ) t2 on t1.k8s_env_name = t2.k8s_env_name
) t1 on t.k8s_env_name = t1.k8s_env_name
left join (
select k8s_env_name,count(1) as total_cnt from cdmdim.$table_name where dt ='${e_dt_var}' group by k8s_env_name
) t2 on t.k8s_env_name = t2.k8s_env_name
left join (
select k8s_env_name,count(1) as nil_cnt from cdmdim.$table_name $null_where_sql_str and dt ='${e_dt_var}' group by k8s_env_name
) t3 on t.k8s_env_name = t3.k8s_env_name
left join (
select k8s_env_name,count(1) as dup_cnt from (select $null_columns,k8s_env_name from cdmdim.$table_name where dt ='${e_dt_var}' group by $null_columns, k8s_env_name having count(1) >1 ) as tmp group by k8s_env_name
) t4 on t.k8s_env_name = t4.k8s_env_name;


select md5(concat('cdmdim','$table_name','${e_dt_var}',k8s_env_name)) as id,* from cdmdq.dim_data_check where dt = '${e_dt_var}' and table_name = '$table_name'; --update_into_pgsql:postgres:cloud1688:172.16.31.1:32086:cdmdq:dim_data_check
