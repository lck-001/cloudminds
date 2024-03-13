set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;

-- 提取并清洗需要字段，计算逻辑，业务背景
with tmp as(
    select
    question_id,
    robot_id,
    asr_agent_id,
    event_time
    from cdmods.ods_hari_event_hitlog_i_h
)
insert overwrite table cdmdwd.dwd_cmd_conversation_i_h partition(dt)
    select
    question_id,
    robot_id,
    asr_agent_id,
    event_time,
    substring(event_time,0,10) as dt
    from tmp
    distribute by dt;