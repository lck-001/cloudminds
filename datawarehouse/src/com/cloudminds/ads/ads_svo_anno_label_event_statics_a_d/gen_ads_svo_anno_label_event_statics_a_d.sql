set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;
set mapreduce.job.queuename=root.prod;
with label_t as (
    SELECT
        a.robot__robot_id,
        a.robot__robot_name,
        a.robot__robot_type_id,
        a.robot__robot_type_inner_name,
        a.robot__robot_type_name,
        a.sv_agent_id,
        a.sv_agent_name,
        a.nlu_event_date,
        a.simple_label_noise_cnt,
        a.valid_conversation_cnt,
        a.expected_ai_ha_conversation_cnt,
        a.expected_ai_conversation_cnt,
        a.asr_recognize_correct_cnt,
        a.expected_ai_conversation_cnt as expected_answer_cnt,
        a.expected_answer_cnt_denominator,
        nvl(b.sla_cnt, 0) as sla_cnt,
        nvl(b.sla_cnt_denominator, 0) as sla_cnt_denominator,
        a.robot__sku,
        a.robot__version_code,
        a.robot__software_version,
        a.robot__hardware_version,
        a.ha_cnt,
        a.tenant__tenant_id
    from
    (
        SELECT
            robot__robot_id,
            robot__robot_name,
            robot__robot_type_id,
            robot__robot_type_inner_name,
            robot__robot_type_name,
            sv_agent_id,
            sv_agent_name,
            to_date(nlu_event_time) as nlu_event_date,
            robot__sku,
            robot__version_code,
            robot__software_version,
            robot__hardware_version,
            tenant__tenant_id,
            sum(if(label_type_id = '5', 1, 0)) as simple_label_noise_cnt,
            sum(CASE label_type_id WHEN '8' THEN 1 ELSE 0 END) + 
            sum(case label_type_id when '3' then 1 else 0 end) + 
            sum(case label_type_id when '1' then 1 else 0 end) + 
            sum(case label_type_id when '2' then 1 else 0 end) +  
            sum(case label_type_id when '4' then 1 else 0 end) + 
            sum(case label_type_id when '6' then 1 else 0 end) +
            sum(case label_type_id when '10' then 1 else 0 end)
            as valid_conversation_cnt,
            sum(CASE label_type_id WHEN '8' THEN 1 ELSE 0 END) + 
            sum(case label_type_id when '1' then 1 else 0 end) + 
            sum(case label_type_id when '4' then 1 else 0 end) +
            sum(case label_type_id when '10' then 1 else 0 end)
            as expected_ai_ha_conversation_cnt,
            sum(CASE label_type_id WHEN '8' THEN 1 ELSE 0 END) + 
            sum(case label_type_id when '1' then 1 else 0 end) + 
            sum(case label_type_id when '10' then 1 else 0 end)
            as expected_ai_conversation_cnt,
            sum(CASE label_type_id WHEN '8' THEN 1 ELSE 0 END) + 
            sum(case label_type_id when '3' then 1 else 0 end)
            as asr_recognize_correct_cnt,
            sum(CASE label_type_id WHEN '8' THEN 1 ELSE 0 END) + 
            sum(case label_type_id when '3' then 1 else 0 end) + 
            sum(case label_type_id when '1' then 1 else 0 end) + 
            sum(case label_type_id when '10' then 1 else 0 end)
            as expected_answer_cnt_denominator,
            sum(case label_type_id WHEN '4' THEN 1 ELSE 0 END) as ha_cnt
        from cdmdwm.dwm_svo_anno_label_event_i_d
        group by
            robot__robot_id,
            robot__robot_name,
            robot__robot_type_id,
            robot__robot_type_inner_name,
            robot__robot_type_name,
            sv_agent_id,
            sv_agent_name,
            to_date(nlu_event_time),
            robot__sku,
            robot__version_code,
            robot__software_version,
            robot__hardware_version,
            tenant__tenant_id
    ) as a left join (
        SELECT
            robot__robot_id,
            robot__robot_name,
            robot__robot_type_id,
            robot__robot_type_inner_name,
            robot__robot_type_name,
            sv_agent_id,
            sv_agent_name,
            to_date(nlu_event_time) as nlu_event_date,
            robot__sku,
            robot__version_code,
            robot__software_version,
            robot__hardware_version,
            tenant__tenant_id,
            sum(CASE label_type_id WHEN '8' THEN 1 ELSE 0 END) + 
            sum(case label_type_id when '1' then 1 else 0 end) + 
            sum(case label_type_id when '4' then 1 else 0 end) +
            sum(case label_type_id when '10' then 1 else 0 end)
            as sla_cnt,
            sum(CASE label_type_id WHEN '8' THEN 1 ELSE 0 END) + 
            sum(case label_type_id when '3' then 1 else 0 end) + 
            sum(case label_type_id when '1' then 1 else 0 end) + 
            sum(case label_type_id when '2' then 1 else 0 end) + 
            sum(case label_type_id when '4' then 1 else 0 end) + 
            sum(case label_type_id when '6' then 1 else 0 end) +
            sum(case label_type_id when '10' then 1 else 0 end)
            as sla_cnt_denominator
        from cdmdwm.dwm_svo_anno_label_event_i_d
        WHERE 
            qa_from in ('user_qa','user_service','sp_qa')
        group by
            robot__robot_id,
            robot__robot_name,
            robot__robot_type_id,
            robot__robot_type_inner_name,
            robot__robot_type_name,
            sv_agent_id,
            sv_agent_name,
            to_date(nlu_event_time),
            robot__sku,
            robot__version_code,
            robot__software_version,
            robot__hardware_version,
            tenant__tenant_id
    ) as b 
        on 
            a.robot__robot_id = b.robot__robot_id 
        and 
            a.robot__robot_name = b.robot__robot_name 
        and 
            a.robot__robot_type_id = b.robot__robot_type_id
        and 
            a.robot__robot_type_inner_name = b.robot__robot_type_inner_name 
        and 
            a.robot__robot_type_name = b.robot__robot_type_name
        and
            a.sv_agent_id = b.sv_agent_id
        and 
            a.sv_agent_name = b.sv_agent_name 
        and 
            a.nlu_event_date = b.nlu_event_date
        and 
            a.robot__sku = b.robot__sku
        and 
            a.robot__version_code = b.robot__version_code 
        and 
            a.robot__software_version = b.robot__software_version
        and 
            a.robot__hardware_version = b.robot__hardware_version
        and 
            a.tenant__tenant_id = b.tenant__tenant_id
),
nlu_t as (
    SELECT
        min(to_date(nlu_event_time)) as min_nlu_event_date,
        max(to_date(nlu_event_time)) as max_nlu_event_date
    from cdmdwm.dwm_svo_anno_label_event_i_d
),
robot_t as (
    SELECT 
        sum(t.robot_online_time) as robot_online_time,
        t.robot__robot_id,
        t.robot__robot_name,
        t.robot__robot_type_id,
        t.robot__robot_type_inner_name,
        t.robot__robot_type_name,
        t.sv_agent_id,
        --t.sv_agent_name,
        t.event_date,
        t.robot__sku,
        t.robot__version_code,
        t.robot__software_version,
        t.robot__hardware_version,
        t.tenant__tenant_id
    from cdmdws.dws_omd_robot_statistic_i_d as t, nlu_t as n
    where 
        t.event_date >= n.min_nlu_event_date
    AND
        t.event_date <= n.max_nlu_event_date
    GROUP by 
        t.robot__robot_id,
        t.robot__robot_name,
        t.robot__robot_type_id,
        t.robot__robot_type_inner_name,
        t.robot__robot_type_name,
        t.sv_agent_id,
        ---t.sv_agent_name,
        t.event_date,
        t.robot__sku,
        t.robot__version_code,
        t.robot__software_version,
        t.robot__hardware_version,
        t.tenant__tenant_id
),
audio_t as (
    SELECT 
        to_date(t.nlu_event_time) as nlu_event_date,
        t.robot__robot_id,
        t.robot__robot_name,
        t.robot__robot_type_id,
        t.robot__robot_type_inner_name,
        t.robot__robot_type_name,
        t.sv_agent_id,
        t.sv_agent_name,
        t.robot__sku,
        t.robot__version_code,
        t.robot__software_version,
        t.robot__hardware_version,
        t.tenant__tenant_id,
        count(distinct question_id) as asr_filter_cnt
    from cdmdwm.dwm_cmd_audio_process_i_d as t, nlu_t as n
    where 
        qa_from = 'asr_filter'
    AND 
        to_date(nlu_event_time) >= n.min_nlu_event_date
    AND 
        to_date(nlu_event_time) <= n.max_nlu_event_date
    GROUP BY 
        to_date(t.nlu_event_time),
        t.robot__robot_id,
        t.robot__robot_name,
        t.robot__robot_type_id,
        t.robot__robot_type_inner_name,
        t.robot__robot_type_name,
        t.sv_agent_id,
        t.sv_agent_name,
        t.robot__sku,
        t.robot__version_code,
        t.robot__software_version,
        t.robot__hardware_version,
        t.tenant__tenant_id
),
sv_project as (
    SELECT 
        trainer_id,
        trainer_name,
        operator_id,
        operator_name,
        project_name,
        priority,
        content_operator_id,
        content_operator_name,
        category,
        scene,
        agent_id,
        dt,
        status,
        status_name
    from (
        SELECT 
            trainer_id,
            trainer_name,
            operator_id,
            operator_name,
            project_name,
            priority,
            content_operator_id,
            content_operator_name,
            category,
            scene,
            dt,
            status,
            status_name,
            sv_agent_id
            from cdmdwm.dwm_svo_project_i_d as t,nlu_t as n
            where         
                dt >= n.min_nlu_event_date
            and
                dt <= n.max_nlu_event_date
    ) m
    LATERAL VIEW OUTER  explode(split(regexp_replace(get_json_object(sv_agent_id, "$.[*].id"),"[\\[\\]]",''),',')) temp  as agent_id
        
),
user_qa_new as (
    select 
        agent_id,
        dt,
        sum(cnt) as qa_new_cnt
    from cdmdws.dws_svo_qa_db_op_i_d,nlu_t as n
    where 
        cnt_cycle = 'day'
    and
        is_del = 0
    and
        qa_type = 'user_qa'
    and 
        op_db = 1
    and
        dt >= n.min_nlu_event_date
    and
        dt <= n.max_nlu_event_date
    GROUP by
        agent_id,
        dt
),
user_qa_total as (
    select
        sv_agent_id,
        sum(total_cnt) as total_cnt
    from cdmads.ads_svo_qa_total_i_d,nlu_t as n
    WHERE
        qa_type = 'user_qa'
    and
        qa_is_del = 0
    and 
        qa_is_work = 1
    and
        dt = '${e_dt_var}'
    GROUP by
        sv_agent_id
)


insert overwrite table cdmads.ads_svo_anno_label_event_statics_a_d
SELECT 
    a.robot__robot_id,
    a.robot__robot_name,
    a.robot__robot_type_id,
    a.robot__robot_type_inner_name,
    a.robot__robot_type_name,
    a.sv_agent_id,
    a.sv_agent_name,
    a.nlu_event_date,
    a.simple_label_noise_cnt,
    a.valid_conversation_cnt,
    a.expected_ai_ha_conversation_cnt,
    a.expected_ai_conversation_cnt,
    a.asr_recognize_correct_cnt,
    a.expected_answer_cnt,
    a.expected_answer_cnt_denominator,
    a.sla_cnt,
    a.sla_cnt_denominator,
    nvl(b.robot_online_time, 0) as robot_online_time,
    nvl(c.asr_filter_cnt, 0) as asr_filter_cnt,
    a.robot__sku,
    a.robot__version_code,
    a.robot__software_version,
    a.robot__hardware_version,
    a.ha_cnt,
    nvl(d.trainer_id, '') as trainer_id,
    nvl(d.trainer_name, '') as trainer_name,
    nvl(d.operator_id, '') as operator_id,
    nvl(d.operator_name, '') as operator_name,
    nvl(d.project_name, '') as project_name,
    nvl(d.priority, '') as priority,
    nvl(d.content_operator_id, '') as content_operator_id,
    nvl(d.content_operator_name, '') as content_operator_name,
    nvl(d.category, '') as category,
    nvl(d.scene, '') as scene,
    nvl(d.status, -99999998) as status,
    nvl(d.status_name, '') as status_name,
    nvl(e.qa_new_cnt, 0) as user_qa_new_cnt,
    nvl(f.total_cnt, 0) as user_qa_total_cnt,
    a.tenant__tenant_id
from (
        SELECT
            robot__robot_id,
            robot__robot_name,
            robot__robot_type_id,
            robot__robot_type_inner_name,
            robot__robot_type_name,
            sv_agent_id,
            sv_agent_name,
            nlu_event_date,
            simple_label_noise_cnt,
            valid_conversation_cnt,
            expected_ai_ha_conversation_cnt,
            expected_ai_conversation_cnt,
            asr_recognize_correct_cnt,
            expected_answer_cnt,
            expected_answer_cnt_denominator,
            sla_cnt,
            sla_cnt_denominator,
            robot__sku,
            robot__version_code,
            robot__software_version,
            robot__hardware_version,
            ha_cnt,
            tenant__tenant_id
        from label_t
    )
    as a left join robot_t as b  
    on 
        a.robot__robot_id = b.robot__robot_id 
    and 
        a.robot__robot_name = b.robot__robot_name 
    and 
        a.robot__robot_type_id = b.robot__robot_type_id
    and 
        a.robot__robot_type_inner_name = b.robot__robot_type_inner_name 
    and 
        a.sv_agent_id = b.sv_agent_id
    -- and 
    --     a.sv_agent_name = b.sv_agent_name 
    and 
        a.nlu_event_date = b.event_date
    and 
        a.robot__robot_type_name = b.robot__robot_type_name
    and 
        a.robot__sku = b.robot__sku
    and 
        a.robot__version_code = b.robot__version_code 
    and 
        a.robot__software_version = b.robot__software_version
    and 
        a.robot__hardware_version = b.robot__hardware_version
    and
        a.tenant__tenant_id = b.tenant__tenant_id
    left join audio_t as c 
    on 
        a.robot__robot_id = c.robot__robot_id 
    and 
        a.robot__robot_name = c.robot__robot_name 
    and 
        a.robot__robot_type_id = c.robot__robot_type_id
    and 
        a.robot__robot_type_inner_name = c.robot__robot_type_inner_name 
    and 
        a.sv_agent_id = c.sv_agent_id
    and 
        a.sv_agent_name = c.sv_agent_name 
    and 
        a.nlu_event_date = c.nlu_event_date
    and 
        a.robot__robot_type_name = c.robot__robot_type_name
    and 
        a.robot__sku = c.robot__sku
    and 
        a.robot__version_code = c.robot__version_code 
    and 
        a.robot__software_version = c.robot__software_version
    and 
        a.robot__hardware_version = c.robot__hardware_version
    and 
        a.tenant__tenant_id = c.tenant__tenant_id
    left join sv_project as d
    on
        a.sv_agent_id = d.agent_id
    and
        a.nlu_event_date = d.dt
    left join user_qa_new as e
    on 
        a.sv_agent_id = e.agent_id
    and
        a.nlu_event_date = e.dt
    left join user_qa_total as f
    on 
        a.sv_agent_id = f.sv_agent_id
