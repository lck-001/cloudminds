set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;
set mapreduce.job.queuename=root.users.liuhao;

with hot_request AS (
    SELECT * from (
        SELECT 
            *,
            row_number() OVER (PARTITION BY intent_name ORDER BY cnt DESC) as rnk
        from (
            SELECT 
                question_text,
                intent_name,
                count(*) as cnt
            from cdmdwd.dwd_nlp_cmd_nlu_recognize_i_d 
            where 
                intent_name is not null 
            AND 
                intent_name != '' 
            AND 
                dt <= '${e_dt_var}'
            AND 
                dt >= date_sub('${e_dt_var}', 30)
            GROUP BY 
                intent_name, 
                question_text
        ) t
    ) c WHERE c.rnk = 1
),
sv_hitlog AS (
    select 
        sv_agent_id,
        robot_id,
        intent_name,
        count(*) as request_cnt,
        k8s_env_name,
        date_format(event_time, 'yyyy-MM-dd') as date_t
    from cdmdwd.dwd_nlp_cmd_nlu_recognize_i_d 
    where 
        intent_name is not null 
    and 
        intent_name != ''
    AND 
        dt = '${e_dt_var}'
    group by
        k8s_env_name,
        intent_name,
        sv_agent_id,
        robot_id,
        date_format(event_time, 'yyyy-MM-dd')
)

insert overwrite table cdmads.ads_cmd_sv_hitlog_i_d partition(dt)
select 
    nvl(b.sv_agent_id, '') as sv_agent_id,
    nvl(b.robot_id, '') as robot_id,
    nvl(a.intent_name, '') as intent_name,
    nvl(a.question_text, '') as tag_request,
    b.request_cnt as cnt,
    b.k8s_env_name as k8s_env_name,
    b.date_t as dt
from (
    (select * from hot_request) as a 
    join 
    (select * from sv_hitlog) as b 
    on a.intent_name = b.intent_name
);