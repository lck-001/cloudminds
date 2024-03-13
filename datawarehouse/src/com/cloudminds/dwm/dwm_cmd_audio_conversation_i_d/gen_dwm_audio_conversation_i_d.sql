set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;
set mapreduce.job.queuename=root.prod;
with raw_set as (
    SELECT
        event_type_id,
        rcu_id,
        robot__robot_type_inner_name,
        robot__robot_id,
        tenant__tenant_id,
        question_id,
        event_time
    from (
            SELECT
                event_type_id,
                rcu_id,
                robot_type as robot__robot_type_inner_name,
                robot_id as robot__robot_id,
                tenant_id as tenant__tenant_id,
                question_id,
                event_time,
                -- case event_type_id WHEN "000020" THEN event_time else '' as video_trigger_time,
                -- case event_type_id WHEN "000021" THEN event_time else '' as voice_start_play_time,
                -- case event_type_id WHEN "000022" THEN event_time else '' as voice_end_play_time
                row_number() over (
                    PARTITION BY question_id
                    order by event_time
                ) as rnk
            from cdmdwd.dwd_sv_cmd_audio_conversation_i_d
            where
                to_date(event_time) >= '${s_dt_var}'
            AND (
                (event_type_id ='000020' AND to_date(event_time) <= '${e_dt_var}')
                OR
                (event_type_id = '000021' AND to_date(event_time) <= date_add('${e_dt_var}', 1))
                OR
                (event_type_id = '000022' AND to_date(event_time) <= date_add('${e_dt_var}', 1))
            )
        ) t
    where t.rnk = 1
),
--语音播报触发事件数据
vocie_trigger as (
    select
        question_id,
        event_time
    from cdmdwd.dwd_sv_cmd_audio_conversation_i_d
    where 
        event_type_id ='000020'
    AND
        to_date(event_time) >= '${s_dt_var}'
    AND
        to_date(event_time) <= '${e_dt_var}'
),
--语音播报开始事件数据
vocie_start_play as (
    select
        question_id,
        event_time
    from cdmdwd.dwd_sv_cmd_audio_conversation_i_d
    where 
        event_type_id ='000021'
    AND
        to_date(event_time) >= '${s_dt_var}'
    AND
        to_date(event_time) <= date_add('${e_dt_var}', 1)
),
--语音播报结束事件数据
vocie_end_play as (
    select
        question_id,
        event_time
    from cdmdwd.dwd_sv_cmd_audio_conversation_i_d
    where 
        event_type_id ='000022'
    AND
        to_date(event_time) >= '${s_dt_var}'
    AND
        to_date(event_time) <= date_add('${e_dt_var}', 1)
)

insert overwrite table cdmdwm.dwm_cmd_audio_conversation_i_d partition(dt)
select
    a.event_type_id as event_type_id,
    a.rcu_id as rcu_id,
    a.robot__robot_type_inner_name as robot__robot_type_inner_name,
    a.robot__robot_id as robot__robot_id,
    a.tenant__tenant_id as tenant__tenant_id,
    a.question_id as question_id,
    nvl(b.event_time, '') as voice_trigger_time,
    nvl(c.event_time, '') as voice_start_play_time,
    nvl(d.event_time, '') as voice_end_play_time,
    to_date(a.event_time) as dt
from raw_set as a
left join vocie_trigger as b
on 
    a.question_id = b.question_id
left join vocie_start_play as c
on
    a.question_id = c.question_id
left join vocie_end_play as d
on
    a.question_id = d.question_id;