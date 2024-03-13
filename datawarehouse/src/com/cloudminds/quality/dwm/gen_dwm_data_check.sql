--i
set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;


with tmp as(
    select
        'cdmdwm' as database_name,
        '$table_name' as table_name,
        '$dim' as dim,
        $id as dim_id
    from cdmdwm.$table_name $null_where_sql_str and dt ='${e_dt_var}'
    ),
    agg as (
select
    database_name,
    table_name,
    str_to_map(concat_ws(',',collect_set(concat_ws(':',dim,cast(cnt as string))))) as dim_uncover_map
from (
    select
    database_name,
    table_name,
    dim,
    count(1) as cnt
    from tmp group by database_name,table_name,dim
    ) t1 group by database_name,table_name
    ),
    dist as (
select
    database_name,
    table_name,
    str_to_map(concat_ws(',',collect_set(concat_ws(':',dim,cast(cnt as string))))) as dist_dim_uncover_map
from (
    select
    database_name,
    table_name,
    dim,
    count(1) as cnt
    from (
    select
    database_name,
    table_name,
    dim,
    dim_id
    from tmp group by database_name,table_name,dim,dim_id
    ) t1 group by database_name,table_name,dim
    ) t2 group by database_name,table_name
    ),
    inc as (
select
    'cdmdwm' as database_name,
    '$table_name' as table_name,
    (t1.cnt1-t2.cnt2) as inc_cnt
from (
    select
    'cdmdwm' as database_name,
    '$table_name' as table_name,
    count(1) as cnt1
    from cdmdwm.$table_name where dt='${e_dt_var}'
    ) t1
    left join (
    select
    'cdmdwm' as database_name,
    '$table_name' as table_name,
    count(1) as cnt2
    from cdmdwm.$table_name where dt=date_sub('${e_dt_var}',1)
    ) t2 on t1.database_name = t2.database_name and t1.table_name = t2.table_name
    ),

    lateral_agg as (
select
    database_name,
    table_name,
    concat('{"', concat_ws('","', collect_list(concat_ws('":"', k1,v1) ) ), '"}') as dim_uncover
from agg
    lateral view outer explode(dim_uncover_map) kv1 as k1,v1
group by database_name,table_name
    ),

    lateral_dist as (
select
    database_name,
    table_name,
    concat('{"', concat_ws('","', collect_list(concat_ws('":"', k2,v2) ) ), '"}') as dist_uncover
from dist
    lateral view outer explode(dist_dim_uncover_map) kv2 as k2,v2
group by database_name,table_name
    ),
    merg as (
select
    t1.database_name,
    split('$table_name','_')[size(split('$table_name','_'))-2] as table_type,
    case when split('$table_name','_')[size(split('$table_name','_'))-2] = 'i' then '增量'
    when split('$table_name','_')[size(split('$table_name','_'))-2] = 's' then '快照'
    when split('$table_name','_')[size(split('$table_name','_'))-2] = 'a' then '全量'
    when split('$table_name','_')[size(split('$table_name','_'))-2] = 'sh' then '拉链'
    else '未知'
    end as table_type_name,
    nvl(t1.dim_uncover,'') as dim_uncover,
    nvl(t2.dist_uncover,'') as dist_uncover,
    nvl(t3.inc_cnt,0) as inc_cnt,
    '${e_dt_var}' as dt,
    t1.table_name
from lateral_agg t1
    left join lateral_dist t2 on t1.database_name = t2.database_name and t1.table_name = t2.table_name
    left join inc t3 on t1.database_name = t3.database_name and t1.table_name = t3.table_name
    )

insert overwrite table cdmdq.dwm_data_check partition(dt,table_name)
select * from merg;

select md5(concat('cdmdwm','$table_name','${e_dt_var}')) as id,* from cdmdq.dwm_data_check where dt = '${e_dt_var}' and table_name = '$table_name'; --update_into_pgsql:postgres:cloud1688:172.16.31.1:32086:cdmdq:dwm_data_check



-- a
set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;


with tmp as(
    select
        'cdmdwm' as database_name,
        '$table_name' as table_name,
        '$dim' as dim,
        $id as dim_id
    from cdmdwm.$table_name $null_where_sql_str
),
     agg as (
         select
             database_name,
             table_name,
             str_to_map(concat_ws(',',collect_set(concat_ws(':',dim,cast(cnt as string))))) as dim_uncover_map
         from (
                  select
                      database_name,
                      table_name,
                      dim,
                      count(1) as cnt
                  from tmp group by database_name,table_name,dim
              ) t1 group by database_name,table_name
     ),
     dist as (
         select
             database_name,
             table_name,
             str_to_map(concat_ws(',',collect_set(concat_ws(':',dim,cast(cnt as string))))) as dist_dim_uncover_map
         from (
                  select
                      database_name,
                      table_name,
                      dim,
                      count(1) as cnt
                  from (
                           select
                               database_name,
                               table_name,
                               dim,
                               dim_id
                           from tmp group by database_name,table_name,dim,dim_id
                       ) t1 group by database_name,table_name,dim
              ) t2 group by database_name,table_name
     ),
     inc as (
         select
             'cdmdwm' as database_name,
             '$table_name' as table_name,
             count(1) as inc_cnt
         from cdmdwm.$table_name
     ),
     lateral_agg as (
         select
             database_name,
             table_name,
             concat('{"', concat_ws('","', collect_list(concat_ws('":"', k1,v1) ) ), '"}') as dim_uncover
         from agg
                  lateral view outer explode(dim_uncover_map) kv1 as k1,v1
group by database_name,table_name
    ),

    lateral_dist as (
select
    database_name,
    table_name,
    concat('{"', concat_ws('","', collect_list(concat_ws('":"', k2,v2) ) ), '"}') as dist_uncover
from dist
    lateral view outer explode(dist_dim_uncover_map) kv2 as k2,v2
group by database_name,table_name
    ),
    merg as (
select
    t1.database_name,
    split('$table_name','_')[size(split('$table_name','_'))-2] as table_type,
    case when split('$table_name','_')[size(split('$table_name','_'))-2] = 'i' then '增量'
    when split('$table_name','_')[size(split('$table_name','_'))-2] = 's' then '快照'
    when split('$table_name','_')[size(split('$table_name','_'))-2] = 'a' then '全量'
    when split('$table_name','_')[size(split('$table_name','_'))-2] = 'sh' then '拉链'
    else '未知'
    end as table_type_name,
    nvl(t1.dim_uncover,'') as dim_uncover,
    nvl(t2.dist_uncover,'') as dist_uncover,
    nvl(t3.inc_cnt,0) as inc_cnt,
    '${e_dt_var}' as dt,
    t1.table_name
from lateral_agg t1
    left join lateral_dist t2 on t1.database_name = t2.database_name and t1.table_name = t2.table_name
    left join inc t3 on t1.database_name = t3.database_name and t1.table_name = t3.table_name
    )

insert overwrite table cdmdq.dwm_data_check partition(dt,table_name)
select * from merg;

select md5(concat('cdmdwm','$table_name','${e_dt_var}')) as id,* from cdmdq.dwm_data_check where dt = '${e_dt_var}' and table_name = '$table_name'; --update_into_pgsql:postgres:cloud1688:172.16.31.1:32086:cdmdq:dwm_data_check




--s
set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;


with tmp as(
    select
        'cdmdwm' as database_name,
        '$table_name' as table_name,
        '$dim' as dim,
        $id as dim_id
    from cdmdwm.$table_name $null_where_sql_str and dt ='${e_dt_var}'
    ),
    agg as (
select
    database_name,
    table_name,
    str_to_map(concat_ws(',',collect_set(concat_ws(':',dim,cast(cnt as string))))) as dim_uncover_map
from (
    select
    database_name,
    table_name,
    dim,
    count(1) as cnt
    from tmp group by database_name,table_name,dim
    ) t1 group by database_name,table_name
    ),
    dist as (
select
    database_name,
    table_name,
    str_to_map(concat_ws(',',collect_set(concat_ws(':',dim,cast(cnt as string))))) as dist_dim_uncover_map
from (
    select
    database_name,
    table_name,
    dim,
    count(1) as cnt
    from (
    select
    database_name,
    table_name,
    dim,
    dim_id
    from tmp group by database_name,table_name,dim,dim_id
    ) t1 group by database_name,table_name,dim
    ) t2 group by database_name,table_name
    ),
    inc as (
select
    'cdmdwm' as database_name,
    '$table_name' as table_name,
    (t1.cnt1-t2.cnt2) as inc_cnt
from (
    select
    'cdmdwm' as database_name,
    '$table_name' as table_name,
    count(1) as cnt1
    from cdmdwm.$table_name where dt='${e_dt_var}'
    ) t1
    left join (
    select
    'cdmdwm' as database_name,
    '$table_name' as table_name,
    count(1) as cnt2
    from cdmdwm.$table_name where dt=date_sub('${e_dt_var}',1)
    ) t2 on t1.database_name = t2.database_name and t1.table_name = t2.table_name
    ),
    lateral_agg as (
select
    database_name,
    table_name,
    concat('{"', concat_ws('","', collect_list(concat_ws('":"', k1,v1) ) ), '"}') as dim_uncover
from agg
    lateral view outer explode(dim_uncover_map) kv1 as k1,v1
group by database_name,table_name
    ),

    lateral_dist as (
select
    database_name,
    table_name,
    concat('{"', concat_ws('","', collect_list(concat_ws('":"', k2,v2) ) ), '"}') as dist_uncover
from dist
    lateral view outer explode(dist_dim_uncover_map) kv2 as k2,v2
group by database_name,table_name
    ),
    merg as (
select
    t1.database_name,
    split('$table_name','_')[size(split('$table_name','_'))-2] as table_type,
    case when split('$table_name','_')[size(split('$table_name','_'))-2] = 'i' then '增量'
    when split('$table_name','_')[size(split('$table_name','_'))-2] = 's' then '快照'
    when split('$table_name','_')[size(split('$table_name','_'))-2] = 'a' then '全量'
    when split('$table_name','_')[size(split('$table_name','_'))-2] = 'sh' then '拉链'
    else '未知'
    end as table_type_name,
    nvl(t1.dim_uncover,'') as dim_uncover,
    nvl(t2.dist_uncover,'') as dist_uncover,
    nvl(t3.inc_cnt,0) as inc_cnt,
    '${e_dt_var}' as dt,
    t1.table_name
from lateral_agg t1
    left join lateral_dist t2 on t1.database_name = t2.database_name and t1.table_name = t2.table_name
    left join inc t3 on t1.database_name = t3.database_name and t1.table_name = t3.table_name
    )

insert overwrite table cdmdq.dwm_data_check partition(dt,table_name)
select * from merg;

select md5(concat('cdmdwm','$table_name','${e_dt_var}')) as id,* from cdmdq.dwm_data_check where dt = '${e_dt_var}' and table_name = '$table_name'; --update_into_pgsql:postgres:cloud1688:172.16.31.1:32086:cdmdq:dwm_data_check
