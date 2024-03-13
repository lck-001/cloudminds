set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set mapreduce.job.queuename=root.users.liuhao;
with temp as (
    SELECT *,
    row_number() over(partition by 
        t.question_id,
        t.event_time
        order by t.event_time
    ) as rnk
    from (
        select 
            get_json_object(raw_message, '$.event_type_id') as event_type_id,
            get_json_object(raw_message, '$.event_name') as event_name,
            get_json_object(raw_message, '$.event_time') as event_time,
            get_json_object(raw_message, '$.model_id') as model_id,
            get_json_object(raw_message, '$.tenant_id') as tenant_id,
            get_json_object(raw_message, '$.option') as option,
            get_json_object(raw_message, '$.operator_id') as operator_id,
            get_json_object(raw_message, '$.project_id') as project_id,
            get_json_object(raw_message, '$.label_type_id') as label_type_id,
            get_json_object(raw_message, '$.label_id') as label_id,
            get_json_object(raw_message, '$.label_name') as label_name,
            get_json_object(raw_message, '$.event_data.question_id') as question_id,
            get_json_object(raw_message, '$.event_data.asr_correct_text') as asr_correct_text,
            get_json_object(raw_message, '$.event_data.audio_correct_text') as audio_correct_text,
            nvl(get_json_object(raw_message, '$.submit_time'), '2021-11-22 00:00:00.000') as submit_time
        from cdmods.ods_cms_event_08_000009_a_d 
        where
            -- asr标注事件 
            get_json_object(raw_message, '$.event_type_id') = '000011'
    ) t
)

insert overwrite table cdmdwd.dwd_cms_svo_asr_label_event_a_d
select 
    nvl(event_type_id, '') as event_type_id,
    nvl(event_name, '') as event_name,
    nvl(substring(event_time, 0, 23), '') as event_time,
    nvl(model_id, '') as model_id,
    nvl(tenant_id, '') as tenant_id,
    nvl(option, '') as option,
    nvl(operator_id, -99999998) as operator_id,
    nvl(project_id, -99999998) as project_id,
    nvl(label_type_id, '') as label_type_id,
    nvl(label_id, '') as label_id,
    nvl(label_name, '') as label_name,
    nvl(question_id, '') as question_id,
    nvl(asr_correct_text, '') as asr_correct_text,
    nvl(audio_correct_text, '') as audio_correct_text,
    substring(submit_time, 0, 23) as submit_time,
    'bj-prod-232' as k8s_env_name
from temp
where rnk = 1;