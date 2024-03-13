set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;

with seats_form as (
    select
        hi_id,
        hi_account,
        k8s_env_name
    from cdmdim.dim_cmd_hi_a_d
    ),
code_form as (
    select
        cast(seat_id as string) as seat_id,
        service_code,
        concat(from_unixtime(cast(event_time/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.000') as event_time,
        case bigdata_method
        when 'c' then 1
        when 'u' then 2
        else 3
        end as db_op,
        k8s_env_name,
        from_unixtime(cast(event_time/1000 as int),'yyyy-MM-dd') as time_dt
    from cdmods.ods_hari_db_c0004_t_seats_service_code_i_d where dt <= '${e_dt_var}' and dt >= '${s_dt_var}'
    union all
    select
            cast(seat_id as string) as seat_id,
            service_code,
            concat(from_unixtime(cast(event_time/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.000') as event_time,
            case bigdata_method
            when 'c' then 1
            when 'u' then 2
            else 3
            end as db_op,
            k8s_env_name,
            from_unixtime(cast(event_time/1000 as int),'yyyy-MM-dd') as time_dt
        from cdmods.ods_hari_db_c0009_t_seats_service_code_i_d where dt <= '${e_dt_var}' and dt >= '${s_dt_var}'
)
insert overwrite table cdmdwd.dwd_harix_cmd_hi_service_i_d partition(dt)
    select
        nvl(code_form.seat_id,'') as hi_id,
        nvl(seats_form.hi_account,'') as account,
        nvl(code_form.service_code,'') as service_code,
        nvl(code_form.event_time,'') as event_time,
        nvl(code_form.db_op,-99999998) as db_op,
        nvl(code_form.k8s_env_name,'') as k8s_env_name,
        nvl(code_form.time_dt,'') as dt
    from code_form
    left join seats_form on code_form.seat_id = seats_form.hi_id;