set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;
set mapreduce.job.queuename=root.prod;
with wakeup_rows as (
    SELECT
        event_type_id,
        robot_id,
        event_time,
        tenant_id,
        rcu_id,
        robot_type,
        wake_up_type,
        wake_up_type_msg
    from cdmdwd.dwd_sv_omd_robot_wakeup_i_d
    where
        to_date(event_time) >= date_sub('${s_dt_var}', 1)
    and
        to_date(event_time) <= '${e_dt_var}'
),
idle_rows as (
    SELECT
        event_type_id,
        robot_id,
        event_time,
        tenant_id,
        rcu_id,
        robot_type,
        0 as wake_up_type,
        '' as wake_up_type_msg
    from cdmdwd.dwd_sv_omd_robot_idle_i_d
    where
        to_date(event_time) >= date_sub('${s_dt_var}', 1)
    and
        to_date(event_time) <= '${e_dt_var}'
),
raw_data as (
    select
        robot__robot_id,
        event_time,
        wake_up_type,
        wake_up_type_msg,
        tenant__tenant_id,
        rcu_id,
        robot__robot_type_inner_name,
        case 
            event_type_id 
        when '000018' then next_time
        else null end as idle_time
    from (
        SELECT
            event_type_id,
            robot_id as robot__robot_id,
            event_time as event_time,
            wake_up_type as wake_up_type,
            wake_up_type_msg as wake_up_type_msg,
            tenant_id as tenant__tenant_id,
            rcu_id as rcu_id,
            robot_type as robot__robot_type_inner_name,
            lead(event_time) over (
                partition by robot_id
                order by event_time
            ) as next_time
        from (
            select * from wakeup_rows as a 
            union all
            select * from idle_rows as b
        ) m
    ) n
)

insert overwrite table cdmdwm.dwm_omd_robot_wakeup_i_d partition(dt)
select
    event_time,
    tenant__tenant_id,
    robot__robot_id,
    rcu_id,
    robot__robot_type_inner_name,
    wake_up_type,
    wake_up_type_msg,
    idle_time,
    to_date(event_time) as dt
from 
    raw_data
where idle_time is not null