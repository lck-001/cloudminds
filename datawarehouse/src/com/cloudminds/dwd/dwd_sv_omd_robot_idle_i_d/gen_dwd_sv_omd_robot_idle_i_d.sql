set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;
set mapreduce.job.queuename=root.prod;

with raw_data as (
    select
        nvl(get_json_object(json_extract, '$.event_type_id'), '') as event_type_id,
        nvl(get_json_object(json_extract, '$.event_name'), '') as event_name,
        nvl(substring(regexp_replace(get_json_object(json_extract, '$.event_time'), 'T', ' '), 0, 19), '') as event_time,
        nvl(get_json_object(json_extract, '$.model_id'), '') as model_id,
        nvl(get_json_object(json_extract, '$.tenant_id'), '') as tenant_id,
        nvl(get_json_object(json_extract, '$.robot_id'), '') as robot_id,
        nvl(get_json_object(json_extract, '$.module_name'), '') as module_name,
        nvl(get_json_object(json_extract, '$.source'), '') as source,
        nvl(get_json_object(json_extract, '$.rcu_id'), '') as rcu_id,
        nvl(get_json_object(json_extract, '$.robot_type'), '') as robot_type,
        nvl(get_json_object(json_extract, '$.option'), '') as option,
        to_date(get_json_object(json_extract, '$.event_time')) as dt
    from cdmods.ods_rcu_event_02_i_d
    where
        get_json_object(json_extract, '$.event_type_id') = '000019'
    and
        to_date(get_json_object(json_extract, '$.event_time')) >= '${s_dt_var}'
    and
        to_date(get_json_object(json_extract, '$.event_time')) <= '${e_dt_var}'
)

insert overwrite table cdmdwd.dwd_sv_omd_robot_idle_i_d partition(dt)
select
    event_type_id,
    event_name,
    event_time,
    model_id,
    tenant_id,
    robot_id,
    module_name,
    source,
    rcu_id,
    robot_type,
    option,
    dt
from (
    select 
        event_type_id,
        event_name,
        event_time,
        model_id,
        tenant_id,
        robot_id,
        module_name,
        source,
        rcu_id,
        robot_type,
        option,
        dt,
        row_number() over (
            partition by robot_id, event_time
            order by event_time desc
        ) as rnk
    from raw_data
) t
where t.rnk = 1;