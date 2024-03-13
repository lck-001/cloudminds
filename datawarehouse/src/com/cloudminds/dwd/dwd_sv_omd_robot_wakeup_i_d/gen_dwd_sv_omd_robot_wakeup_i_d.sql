set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;
set mapreduce.job.queuename=root.prod;
insert overwrite table cdmdwd.dwd_sv_omd_robot_wakeup_i_d partition(dt)
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
    nvl(get_json_object(json_extract, '$.event_data.wake_up_type'), -99999998) as wake_up_type,
    case get_json_object(json_extract, '$.event_data.wake_up_type')
    when 0 then '人脸'
    when 1 then '语音'
    when 2 then 'HA'
    when 3 then '触屏'
    else '未知' 
    end as wake_up_type_msg,
    to_date(get_json_object(json_extract, '$.event_time')) as dt
from cdmods.ods_rcu_event_02_i_d
where
    get_json_object(json_extract, '$.event_type_id') = '000018'
and
    to_date(get_json_object(json_extract, '$.event_time')) >= '${s_dt_var}'
and
    to_date(get_json_object(json_extract, '$.event_time')) <= '${e_dt_var}';

    