set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;

insert overwrite table cdmdwd.dwd_harix_cmd_hi_login_i_d partition (dt)
select
    nvl(seat_id,'') as hi_id,
    nvl(login_id,'') as account,
    nvl(concat(from_unixtime(cast(event_time/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'') as event_time,
    case bigdata_method
            when 'c' then 1
            when 'u' then 2
    else 3
    end as db_op,
    nvl(k8s_env_name,'') as k8s_env_name,
    nvl(from_unixtime(cast(event_time/1000 as int),'yyyy-MM-dd'),'') as dt
from (select
            cast(seat_id as string) as seat_id,
            login_id,
            event_time,
            bigdata_method,
            k8s_env_name,
            dt
     from cdmods.ods_hari_db_c0004_t_login_seats_i_d
     where dt <= '${e_dt_var}' and dt >= '${s_dt_var}'
     union all
     select
           cast(seat_id as string) as seat_id,
           login_id,
           event_time,
           bigdata_method,
           k8s_env_name,
           dt
     from cdmods.ods_hari_db_c0009_t_login_seats_i_d
     where dt <= '${e_dt_var}' and dt >= '${s_dt_var}'
) union_table ;

