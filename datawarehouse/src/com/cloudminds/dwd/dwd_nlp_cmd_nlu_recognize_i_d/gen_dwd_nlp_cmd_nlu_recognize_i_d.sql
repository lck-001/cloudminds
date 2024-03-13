set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;
set mapreduce.job.queuename=root.users.liuhao;

-- 原始数据读取
with nlu_recognize_tmp as (
    select
        case when nvl(GET_JSON_OBJECT(data_json,'$.parsed.Questionid'),'') != '' then nvl(GET_JSON_OBJECT(data_json,'$.parsed.Questionid'),'')
             else nvl(GET_JSON_OBJECT(data_json,'$.parsed.Traceid'),'')
        end as question_id,
        nvl(GET_JSON_OBJECT(data_json,'$.parsed.Robotid'),'')  as robot_id,
        nvl(GET_JSON_OBJECT(data_json,'$.parsed.Envinfo.devicetype'),'') as robot_type,
        case when nvl(GET_JSON_OBJECT(data_json,'$.parsed.Username'),'')  != '' then nvl(GET_JSON_OBJECT(data_json,'$.parsed.Username'),'')
             else nvl(GET_JSON_OBJECT(data_json,'$.parsed.Sessionid'),'')
        end as robot_account_id,
        nvl(GET_JSON_OBJECT(data_json,'$.parsed.Agentid'),'') as sv_agent_id,
        nvl(GET_JSON_OBJECT(data_json,'$.parsed.Agentname'),'') as sv_agent_name,
        nvl(GET_JSON_OBJECT(data_json,'$.parsed.Algo'),'') as algo,
        cast(nvl(GET_JSON_OBJECT(data_json,'$.parsed.Cost'),-99999998) as int) as cost,
        nvl(GET_JSON_OBJECT(data_json,'$.parsed.MatchedTemplate.id'),'') as matched_template_id,
        nvl(GET_JSON_OBJECT(data_json,'$.parsed.MatchedTemplate.template'),'') as matched_template,
        nvl(GET_JSON_OBJECT(data_json,'$.parsed.Domainid'),'') as domain_id,
        nvl(GET_JSON_OBJECT(data_json,'$.parsed.Domain'),'') as domain_name,
        nvl(GET_JSON_OBJECT(data_json,'$.parsed.Intentid'),'') as intent_id,
        nvl(GET_JSON_OBJECT(data_json,'$.parsed.Intent'),'') as intent_name,
        nvl(GET_JSON_OBJECT(data_json,'$.parsed.Envinfo'),'') as env_info,
        case when nvl(GET_JSON_OBJECT(data_json,'$.parsed.BeforeContext.usercontext'),'') = '' and nvl(GET_JSON_OBJECT(data_json,'$.parsed.BeforeContext.systemcontext'),'') = '' then ''
             else nvl(GET_JSON_OBJECT(data_json,'$.parsed.BeforeContext'),'')
        end as before_context,
        nvl(GET_JSON_OBJECT(data_json,'$.parsed.InContext'),'') as in_context,
        nvl(GET_JSON_OBJECT(data_json,'$.parsed.OutContext'),'') as out_context,
        case when nvl(GET_JSON_OBJECT(data_json,'$.parsed.ParamInfo'),'') = '[]' then ''
             else nvl(GET_JSON_OBJECT(data_json,'$.parsed.ParamInfo'),'')
        end as param_info,
        case when nvl(GET_JSON_OBJECT(data_json,'$.parsed.Parameters'),'') = '{}' then ''
             else nvl(GET_JSON_OBJECT(data_json,'$.parsed.Parameters'),'')
        end as parameters,
        nvl(GET_JSON_OBJECT(data_json,'$.parsed.QaResult'),'') as qa_result,
        case when nvl(GET_JSON_OBJECT(data_json,'$.parsed.QaResult.answer.question.typeid'),'') != '' and trim(nvl(GET_JSON_OBJECT(data_json,'$.parsed.Source'),'')) = 'common_sense_qa' then nvl(GET_JSON_OBJECT(data_json,'$.parsed.QaResult.answer.question.typeid'),'')
            else ''
        end as fqa_cate_id,
        case when nvl(GET_JSON_OBJECT(data_json,'$.parsed.QaResult.answer.question.typeid'),'') != '' and trim(nvl(GET_JSON_OBJECT(data_json,'$.parsed.Source'),'')) = 'user_qa' then nvl(GET_JSON_OBJECT(data_json,'$.parsed.QaResult.answer.question.typeid'),'')
             else ''
        end as user_qa_cate_id,
        nvl(GET_JSON_OBJECT(data_json,'$.parsed.QaResult.answer.question.typeid'),'') as answer_type_id,
        case when nvl(GET_JSON_OBJECT(data_json,'$.parsed.QaResult.answer.qgroupid'),'') != '' then nvl(GET_JSON_OBJECT(data_json,'$.parsed.QaResult.answer.qgroupid'),'')
             else nvl(GET_JSON_OBJECT(data_json,'$.parsed.QaResult.answer.question.qgroupid'),'')
        end as question_group_id,
        case when nvl(GET_JSON_OBJECT(data_json,'$.parsed.QaResult.answer.score'),'') != '' then cast(nvl(GET_JSON_OBJECT(data_json,'$.parsed.QaResult.answer.score'),0.0000000000000001) as decimal(18,16))
             else cast(nvl(GET_JSON_OBJECT(data_json,'$.parsed.QaResult.answer.question.algoscore'),0.0000000000000001) as decimal(18,16))
        end as sim_question_score,
        regexp_replace(nvl(GET_JSON_OBJECT(data_json,'$.parsed.Request'),''), '\r|\n|\t', '') as question_text,
        regexp_replace(nvl(GET_JSON_OBJECT(data_json,'$.parsed.Response'),''), '\r|\n|\t', '') as answer_text,
        nvl(GET_JSON_OBJECT(data_json,'$.parsed.Sessionid'),'') as session_id,
        nvl(GET_JSON_OBJECT(data_json,'$.parsed.Source'),'') as qa_from,
        case when GET_JSON_OBJECT(data_json,'$.parsed.is_ha') = true or GET_JSON_OBJECT(data_json,'$.parsed.is_ha') = 'true' then 1
             else 0
        end as is_hi,
        nvl(GET_JSON_OBJECT(data_json,'$.parsed.VprId'),'') as vpr_id,
        nvl(GET_JSON_OBJECT(data_json,'$.parsed.Emotion'),'') as emotion,
        case when GET_JSON_OBJECT(data_json,'$.parsed.Rc') = false or GET_JSON_OBJECT(data_json,'$.parsed.Rc') = 'false' then 0
             else 1
        end as is_rc,
        nvl(GET_JSON_OBJECT(data_json,'$.parsed.Supplier'),'') as supplier,
        nvl(GET_JSON_OBJECT(data_json,'$.parsed.Suppliertype'),'') as supplier_type,
        cast(nvl(GET_JSON_OBJECT(data_json,'$.parsed.ThirdCost'),-99999998) as int) as third_cost,
        nvl(GET_JSON_OBJECT(data_json,'$.parsed.Thirdsessionid'),'') as third_session_id,
        nvl(GET_JSON_OBJECT(data_json,'$.k8s_svc_name'),'') as k8s_svc_name,
        nvl(GET_JSON_OBJECT(data_json,'$.k8s_env_name'),'') as k8s_env_name,
        case when nvl(from_utc_timestamp(substring(GET_JSON_OBJECT(data_json,'$.parsed.Time'),0,23),'PRC'),'') != '' then nvl(from_utc_timestamp(substring(GET_JSON_OBJECT(data_json,'$.parsed.Time'),0,23),'PRC'),'')
             when length(trim(substring(GET_JSON_OBJECT(data_json,'$.parsed.Time'),0,23))) = 22 then nvl(concat(from_utc_timestamp(concat(substring(GET_JSON_OBJECT(data_json,'$.parsed.Time'),0,22),'0'),'PRC'),'0'),'')
             else nvl(substring(GET_JSON_OBJECT(data_json,'$.formated_time'),0,23),'')
        end as event_time,
        data_json as ext,
        case when nvl(from_utc_timestamp(substring(GET_JSON_OBJECT(data_json,'$.parsed.Time'),0,23),'PRC'),'') != '' then nvl(substring(from_utc_timestamp(substring(GET_JSON_OBJECT(data_json,'$.parsed.Time'),0,23),'PRC'),0,10),'')
             when length(trim(substring(GET_JSON_OBJECT(data_json,'$.parsed.Time'),0,23))) = 22 then nvl(substring(from_utc_timestamp(concat(substring(GET_JSON_OBJECT(data_json,'$.parsed.Time'),0,22),'0'),'PRC'),0,10),'')
             else nvl(substring(GET_JSON_OBJECT(data_json,'$.formated_time'),0,10),'')
        end as dt
    from sv.hitlog_source
    where tdate >= date_sub('${s_dt_var}',2) and tdate <= '${e_dt_var}'
),
-- 存在数据重复情况,需要去重 按照asr_ctrl 规则8-8 数据量只有10400，而实际命中的hitlog有14000
nlu_recognize as (
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
        dt,
        row_number() over(partition by question_id order by event_time asc) as rnk
    from nlu_recognize_tmp
    where substring(event_time,0,10) >= date_sub('${s_dt_var}',2) and substring(event_time,0,10) <= '${e_dt_var}'
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
        dt
    from nlu_recognize
    where rnk = 1;