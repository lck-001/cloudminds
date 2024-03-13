set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;
set mapreduce.job.queuename=root.users.liuhao;

with ods_hitlog_d as (
    select
        nvl(question_id,'') as question_id,
        nvl(robotid,'') as robot_id,
        nvl(GET_JSON_OBJECT(envinfo,'$.devicetype'),'') as robot_type,
        nvl(username,'') as robot_account_id,
        nvl(agentid,'') as sv_agent_id,
        nvl(agentname,'') as sv_agent_name,
        nvl(algo,'') as algo,
        nvl(cost,-99999998) as cost,
        nvl(GET_JSON_OBJECT(matched_template,'$.id'),'') as matched_template_id,
        nvl(GET_JSON_OBJECT(matched_template,'$.template'),'') as matched_template,
        nvl(domainid,'') as domain_id,
        nvl(domain,'') as domain_name,
        nvl(intent_id,'') as intent_id,
        nvl(intent,'') as intent_name,
        case when nvl(envinfo,'') = '{}' or nvl(envinfo,'') = '' then ''
             else nvl(envinfo,'')
        end as env_info,
        nvl(before_context,'') as before_context,
        nvl(in_context,'') as in_context,
        nvl(out_context,'') as out_context,
        nvl(param_info,'') as param_info,
        case when nvl(parameters,'') = '{}' or nvl(parameters,'') = '' then ''
             else nvl(parameters,'')
        end as parameters,
        nvl(qa_result,'') as qa_result,
        case when nvl(GET_JSON_OBJECT(qa_result,'$.answer.question.typeid'),'') != '' and trim(Source) = 'common_sense_qa' then nvl(GET_JSON_OBJECT(qa_result,'$.answer.question.typeid'),'')
             else ''
        end as fqa_cate_id,
        case when nvl(GET_JSON_OBJECT(qa_result,'$.answer.question.typeid'),'') != '' and trim(source) = 'user_qa' then nvl(GET_JSON_OBJECT(qa_result,'$.answer.question.typeid'),'')
             else ''
        end as user_qa_cate_id,
        case when nvl(GET_JSON_OBJECT(qa_result,'$.answer.qgroupid'),'') != '' then nvl(GET_JSON_OBJECT(qa_result,'$.answer.qgroupid'),'')
             else nvl(GET_JSON_OBJECT(qa_result,'$.answer.question.qgroupid'),'')
        end as question_group_id,
        case when nvl(GET_JSON_OBJECT(qa_result,'$.answer.score'),'') != '' then cast(nvl(GET_JSON_OBJECT(qa_result,'$.answer.score'),0.0000000000000001) as decimal(18,16))
             else cast(nvl(GET_JSON_OBJECT(qa_result,'$.answer.question.algoscore'),0.0000000000000001) as decimal(18,16))
        end as sim_question_score,
        lower(regexp_replace(nvl(trim(request),''), '\r|\n|\t', '')) as question_text,
        lower(regexp_replace(nvl(trim(response),''), '\r|\n|\t', '')) as answer_text,
        nvl(sessionid,'') as session_id,
        nvl(source,'') as qa_from,
        0 as is_hi,
        '' as vpr_id,
        '' as emotion,
        1 as is_rc,
        nvl(supplier,'') as supplier,
        nvl(suppliertype,'') as supplier_type,
        nvl(third_cost,-99999998) as third_cost,
        nvl(third_sessionid,'') as third_session_id,
        '' as k8s_svc_name,
        nvl(k8s_env_name,'') as k8s_env_name,
        case when length(time) = 19 then concat(time,'.000')
             when length(time) > 19 and length(time) < 23 then nvl(rpad(REGEXP_REPLACE(time,'T',' '),23,'0'),'')
             when length(time) >= 23 then SUBSTRING(REGEXP_REPLACE(time,'T',' '),0,23)
             else ''
        end as event_time,
        '' as ext
    from cdm_sv.ods_hitlog_d
    where tdate >= '${s_dt_var}' and tdate <= '${e_dt_var}' and (trim(lower(regexp_replace(nvl(trim(request),''), '\r|\n|\t', ''))) != '' or trim(lower(regexp_replace(nvl(trim(response),''), '\r|\n|\t', ''))) != '')
)

insert overwrite table cdmdwd.dwd_nlp_cmd_nlu_recognize_i_d partition(dt)
    select
        question_id,
        robot_id,
        robot_type,
        robot_account_id,
        sv_agent_id,
        sv_agent_name,
        algo,
        cost,
        matched_template_id,
        matched_template,
        domain_id,
        domain_name,
        intent_id,
        intent_name,
        env_info,
        before_context,
        in_context,
        out_context,
        param_info,
        parameters,
        qa_result,
        fqa_cate_id,
        user_qa_cate_id,
        question_group_id,
        sim_question_score,
        question_text,
        answer_text,
        session_id,
        qa_from,
        is_hi,
        vpr_id,
        emotion,
        is_rc,
        supplier,
        supplier_type,
        third_cost,
        third_session_id,
        k8s_svc_name,
        k8s_env_name,
        event_time,
        ext,
        to_date(event_time) as dt
    from ods_hitlog_d;
