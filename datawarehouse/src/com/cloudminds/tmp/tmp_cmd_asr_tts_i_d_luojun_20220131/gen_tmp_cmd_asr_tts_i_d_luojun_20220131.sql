set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;
set mapreduce.job.queuename=root.users.hive;

with union_question_id as (
    select
        question_id
    from (
         select question_id from cdm_sv.ods_hitlog_d  where tdate >= date_sub('${s_dt_var}',2) and tdate <= '${e_dt_var}'
         union all
         select question_id from test.tmp_dwd_asr_cmd_asr_i_d  where dt >= date_sub('${s_dt_var}',2) and dt <= '${e_dt_var}'
         union all
         select guid as question_id from cdmdwd.dwd_tts_cmd_tts_i_d  where dt >= date_sub('${s_dt_var}',2) and dt <= '${e_dt_var}'
    ) t where trim(t.question_id) != '' group by question_id
),
join_data as (
    select
    t1.question_id,
    case when nvl(t2.robot_id,'') != '' then nvl(t2.robot_id,'')
         when nvl(t3.robot_id,'') != '' then nvl(t3.robot_id,'')
         else nvl(t4.robot_id,'')
    end as robot_id,
    nvl(t2.agent_id,'') as agent_id,
    nvl(t2.agent_name,'') as agent_name,
    nvl(t2.algo,'') as algo,
    nvl(t2.before_context,'') as before_context,
    nvl(t2.in_context,'') as in_context,
    nvl(t2.out_context,'') as out_context,
    nvl(cast(cost as int),0) as cost,
    nvl(t2.intent_id,'') as intent_id,
    nvl(t2.intent_name,'') as intent_name,
    nvl(t2.domain_id,'') as domain_id,
    nvl(t2.domain_name,'') as domain_name,
    nvl(t2.env_info,'') as env_info,
    nvl(t2.matched_template,'') as matched_template,
    nvl(t2.param_info,'') as param_info,
    nvl(t2.parameters,'') as parameters,
    nvl(t2.qa_result,'') as qa_result,
    nvl(t2.session_id,'') as session_id,
    nvl(t2.qa_from,'') as qa_from,
    nvl(t2.supplier,'') as supplier,
    nvl(t2.supplier_type,'') as supplier_type,
    nvl(cast(third_cost as int),0) as third_cost,
    nvl(t2.third_session_id,'') as third_session_id,
    nvl(t2.robot_account_id,'') as robot_account_id,
    nvl(t2.request,'') as request,
    nvl(t2.response,'') as response,
    nvl(t2.time,'') as t1_event_time,
    nvl(t2.k8s_env_name,'') as t1_k8s_env_name,
    nvl(t3.asr_text,'') as as_text,
    nvl(t3.audio_file_url,'') as asr_audio_url,
    nvl(t3.event_time,'') as t2_event_time,
    nvl(t3.k8s_env_name,'') as t2_k8s_env_name,
    nvl(t4.tts_text,'') as tts_text,
    nvl(t4.audio_url,'') as tts_audio_url,
    nvl(t4.event_time,'') as t3_event_time,
    nvl(t4.k8s_env_name,'') as t3_k8s_env_name,
    case when t2.k8s_env_name is not null and trim(t2.k8s_env_name) != '' then t2.k8s_env_name
         when t3.k8s_env_name is not null and trim(t3.k8s_env_name) != '' then t3.k8s_env_name
    else nvl(t4.k8s_env_name,'')
    end as k8s_env_name,
    least(if(nvl(t2.time,'') = '','9999-12-31 23:59:59.999',t2.time),
          if(nvl(t3.event_time,'') = '','9999-12-31 23:59:59.999',t3.event_time),
          if(nvl(t4.event_time,'') = '','9999-12-31 23:59:59.999',t4.event_time)) as cmd_start_time,
    greatest(if(nvl(t2.time,'') = '','',t2.time),
             if(nvl(t3.event_time,'') = '','',t3.event_time),
             if(nvl(t4.event_time,'') = '','',t4.event_time)) as cmd_end_time
    from union_question_id t1
    left join (
        select
            question_id,
            agentid as agent_id,
            agentname as agent_name,
            algo,
            before_context,
            in_context,
            out_context,
            cost,
            intent_id,
            intent as intent_name,
            domainid as domain_id,
            domain as domain_name,
            envinfo as env_info,
            matched_template,
            param_info,
            parameters,
            qa_result,
            robotid as robot_id,
            sessionid as session_id,
            source as qa_from,
            supplier,
            suppliertype as supplier_type,
            third_cost,
            third_sessionid as third_session_id,
            username as robot_account_id,
            lower(regexp_replace(nvl(trim(request),''), '\r|\n|\t', '')) as request,
            lower(regexp_replace(nvl(trim(response),''), '\r|\n|\t', '')) as response,
            time,
            k8s_env_name
        from cdm_sv.ods_hitlog_d
        where tdate >= date_sub('${s_dt_var}',2) and tdate <= '${e_dt_var}'
    ) t2 on t1.question_id = t2.question_id
    left join (
        select
            question_id,
            robot_id,
            lower(regexp_replace(nvl(trim(asr_text),''), '\r|\n|\t', '')) as asr_text,
            audio_file_url,
            event_time,
            k8s_env_name
        from test.tmp_dwd_asr_cmd_asr_i_d
        where dt >= date_sub('${s_dt_var}',2) and dt <= '${e_dt_var}'
    ) t3 on t1.question_id = t3.question_id
    left join (
        select
            guid as question_id,
            robot_id,
            lower(regexp_replace(nvl(trim(text),''), '\r|\n|\t', '')) as tts_text,
            audio as audio_url,
            event_time,
            k8s_env_name
        from cdmdwd.dwd_tts_cmd_tts_i_d
        where dt >= date_sub('${s_dt_var}',2) and dt <= '${e_dt_var}'
    ) t4 on t1.question_id = t4.question_id
)
insert overwrite table cdmtmp.tmp_cmd_asr_tts_i_d_luojun_20220131 partition(dt)
    select
        question_id,
        robot_id,
        agent_id,
        agent_name,
        algo,
        before_context,
        in_context,
        out_context,
        cost,
        intent_id,
        intent_name,
        domain_id,
        domain_name,
        env_info,
        matched_template,
        param_info,
        parameters,
        qa_result,
        session_id,
        qa_from,
        supplier,
        supplier_type,
        third_cost,
        third_session_id,
        robot_account_id,
        request,
        response,
        t1_event_time,
        t1_k8s_env_name,
        as_text,
        asr_audio_url,
        t2_event_time,
        t2_k8s_env_name,
        tts_text,
        tts_audio_url,
        t3_event_time,
        t3_k8s_env_name,
        k8s_env_name,
        cmd_start_time,
        cmd_end_time,
        to_date(cmd_start_time) as dt
    from join_data;