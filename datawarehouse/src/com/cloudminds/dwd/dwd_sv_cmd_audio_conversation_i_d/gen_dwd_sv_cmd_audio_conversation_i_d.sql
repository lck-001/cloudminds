set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;
set mapreduce.job.queuename=root.prod;
insert overwrite table cdmdwd.dwd_sv_cmd_audio_conversation_i_d partition(dt)
select
    nvl(get_json_object(json_extract, '$.event_type_id'), '') as event_type_id,
    nvl(get_json_object(json_extract, '$.event_name'), '') as event_name,
    nvl(substring(regexp_replace(get_json_object(json_extract, '$.event_time'), 'T', ' '), 0, 19), '') as event_time,
    nvl(get_json_object(json_extract, '$.model_id'), '') as model_id,
    nvl(get_json_object(json_extract, '$.tenant_id'), '') as tenant_id,
    nvl(get_json_object(json_extract, '$.option'), '') as option,
    nvl(get_json_object(json_extract, '$.robot_id'), '') as robot_id,
    nvl(get_json_object(json_extract, '$.module_name'), '') as module_name,
    nvl(get_json_object(json_extract, '$.source'), '') as source,
    nvl(get_json_object(json_extract, '$.rcu_id'), '') as rcu_id,
    nvl(get_json_object(json_extract, '$.robot_type'), '') as robot_type,
    nvl(get_json_object(json_extract, '$.event_data.question_id'), '') as question_id,
    to_date(get_json_object(json_extract, '$.event_time')) as dt
from cdmods.ods_rcu_event_02_i_d
where 
    to_date(get_json_object(json_extract, '$.event_time')) >= '${s_dt_var}'
and
    to_date(get_json_object(json_extract, '$.event_time')) <= '${e_dt_var}'
and
    -- 000020 语音触发事件
    -- 000021 语音响应事件
    -- 000022 机器人结束语音响应事件
    -- see http://172.16.31.107:8888/cloudminds/dataspec/#/rcuevent
    get_json_object(json_extract, '$.event_type_id') in ('000020', '000021', '000022')