set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;
set mapreduce.input.fileinputformat.input.dir.recursive=true;
set  hive.exec.max.dynamic.partitions.pernode=500;
set hive.merge.smallfiles.avgsize=128000000;
set hive.merge.size.per.task=128000000;
set mapreduce.job.queuename=root.users.liuhao;

with asrctrl_tmp as (
    select
        json_extract,
        k8s_svc_name,
        k8s_env_name,
        tdate,
        event_time
    from (
         select
             json_extract,
             k8s_svc_name,
             k8s_env_name,
             tdate,
             case when length(GET_JSON_OBJECT(json_extract,'$.parsed.ts')) = 10 then nvl(concat(from_unixtime( cast(GET_JSON_OBJECT(json_extract,'$.parsed.ts') as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
                  when length(GET_JSON_OBJECT(json_extract,'$.parsed.ts')) >= 23 then nvl(from_utc_timestamp(SUBSTRING(REGEXP_REPLACE(GET_JSON_OBJECT(json_extract,'$.parsed.ts'),'T',' ') ,0,23),'PRC'),'')
                  else nvl(concat(from_unixtime( cast(GET_JSON_OBJECT(json_extract,'$.parsed.ts')/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(GET_JSON_OBJECT(json_extract,'$.parsed.ts'),11,3)),'')
             end as event_time
         from sv.asr_ctrl
         where tdate >= date_sub('${s_dt_var}',2) and tdate <= '${e_dt_var}'
    ) t  where substring(event_time,0,10) >= date_sub('${s_dt_var}',2) and substring(event_time,0,10) <= '${e_dt_var}'
),
asrctrl as (
    select
        case when nvl(GET_JSON_OBJECT(json_extract,'$.parsed.questionId'),'') != '' then nvl(GET_JSON_OBJECT(json_extract,'$.parsed.questionId'),'')
             else nvl(trim(GET_JSON_OBJECT(json_extract,'$.parsed.header.rootGuid')),'')
        end as question_id,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.parsed.header.robotId')),'') as robot_id,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.parsed.header.userId')),'') as robot_account_id,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.parsed.header.version')),'') as version,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.parsed.header.serviceCode')),'') as service_code,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.parsed.header.tenantId')),'') as tenant_id,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.parsed.header.robotType')),'') as robot_type,
        'sv_hit_log' as rod_type,
        regexp_replace(nvl(trim(GET_JSON_OBJECT(json_extract,'$.parsed.question')),''), '\r|\n|\t', '') as question_text,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.parsed.asr_domain')),'') as asr_domain,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.parsed.asr_third')),-99999998) as asr_third_total,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.parsed.asr_vendor')),'') as asr_vendor,
        case when length(GET_JSON_OBJECT(json_extract,'$.parsed.asrStart')) = 10 then nvl(concat(from_unixtime( cast(GET_JSON_OBJECT(json_extract,'$.parsed.asrStart') as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
             when length(GET_JSON_OBJECT(json_extract,'$.parsed.asrStart')) >= 23 then nvl(from_utc_timestamp(SUBSTRING(REGEXP_REPLACE(GET_JSON_OBJECT(json_extract,'$.parsed.asrStart'),'T',' ') ,0,23),'PRC'),'')
             else nvl(concat(from_unixtime( cast(GET_JSON_OBJECT(json_extract,'$.parsed.asrStart')/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(GET_JSON_OBJECT(json_extract,'$.parsed.asrStart'),11,3)),'')
        end as asr_start,
        case when length(GET_JSON_OBJECT(json_extract,'$.parsed.asrEnd')) = 10 then nvl(concat(from_unixtime( cast(GET_JSON_OBJECT(json_extract,'$.parsed.asrEnd') as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
             when length(GET_JSON_OBJECT(json_extract,'$.parsed.asrEnd')) >= 23 then nvl(from_utc_timestamp(SUBSTRING(REGEXP_REPLACE(GET_JSON_OBJECT(json_extract,'$.parsed.asrEnd'),'T',' ') ,0,23),'PRC'),'')
             else nvl(concat(from_unixtime( cast(GET_JSON_OBJECT(json_extract,'$.parsed.asrEnd')/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(GET_JSON_OBJECT(json_extract,'$.parsed.asrEnd'),11,3)),'')
        end as asr_end,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.parsed.nlufwd')),'') as nlufwd,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.parsed.agent_id')),'') as sv_agent_id,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.parsed.wake_flag')),-99999998) as wake_status,
        case when GET_JSON_OBJECT(json_extract,'$.parsed.wake_flag') = -1 then '异常'
             when GET_JSON_OBJECT(json_extract,'$.parsed.wake_flag') = 0 then '休眠'
             when GET_JSON_OBJECT(json_extract,'$.parsed.wake_flag') = 1 then '唤醒'
             when nvl(trim(GET_JSON_OBJECT(json_extract,'$.parsed.wake_flag')),-99999998) = -99999998 then '未知'
        end as wake_status_name,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.parsed.asrctrl_nlp_total')),-99999998) as nlp_total,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.parsed.type')),'') as log_module,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.parsed.gap')),-99999998) as gap,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.parsed.vocal_total')),-99999998) as vocal_total,
        case when length(GET_JSON_OBJECT(json_extract,'$.parsed.lastPreendStart')) = 10 then nvl(concat(from_unixtime( cast(GET_JSON_OBJECT(json_extract,'$.parsed.lastPreendStart') as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
             when length(GET_JSON_OBJECT(json_extract,'$.parsed.lastPreendStart')) >= 23 then nvl(from_utc_timestamp(SUBSTRING(REGEXP_REPLACE(GET_JSON_OBJECT(json_extract,'$.parsed.lastPreendStart'),'T',' ') ,0,23),'PRC'),'')
             else nvl(concat(from_unixtime( cast(GET_JSON_OBJECT(json_extract,'$.parsed.lastPreendStart')/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(GET_JSON_OBJECT(json_extract,'$.parsed.lastPreendStart'),11,3)),'')
        end as last_pre_end_start,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.parsed.asrctrl_duration')),-99999998) as asrctrl_duration,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.parsed.x-b3-traceid')),'') as x_b3_traceid,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.parsed.asrctrl_inner')),-99999998) as asrctrl_inner,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.parsed.caller')),'') as caller,
        case when trim(GET_JSON_OBJECT(json_extract,'$.parsed.hasPreEnd')) = false or trim(GET_JSON_OBJECT(json_extract,'$.parsed.hasPreEnd')) = 'false' then 0
             else 1
        end as is_pre_end,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.parsed.vpr_id')),'') as vpr_id,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.parsed.asrctrl_robotskill_total')),-99999998) as asrctrl_robot_skill_total,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.parsed.asrctrl_total')),-99999998) as asr_nlp_total,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.parsed.size')),-99999998) as audio_size,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.k8s_svc_name')),'') as k8s_svc_name,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.k8s_env_name')),'') as k8s_env_name,
        event_time,
        json_extract as ext,
        case when length(GET_JSON_OBJECT(json_extract,'$.parsed.ts')) = 13 then from_unixtime( cast(GET_JSON_OBJECT(json_extract,'$.parsed.ts')/1000 as int),'yyyy-MM-dd')
             when length(GET_JSON_OBJECT(json_extract,'$.parsed.ts')) = 10 then from_unixtime( cast(GET_JSON_OBJECT(json_extract,'$.parsed.ts') as int),'yyyy-MM-dd')
             else nvl(substring(from_utc_timestamp(SUBSTRING(REGEXP_REPLACE(GET_JSON_OBJECT(json_extract,'$.parsed.ts'),'T',' ') ,0,23),'PRC'),0,10) ,'')
        end as dt
    from asrctrl_tmp
),
asrctrl_remove_duplicate as (
    select
        question_id,
        robot_id,
        robot_account_id,
        version,
        service_code,
        tenant_id,
        robot_type,
        rod_type,
        question_text,
        asr_domain,
        asr_third_total,
        asr_vendor,
        asr_start,
        asr_end,
        nlufwd,
        sv_agent_id,
        wake_status,
        wake_status_name,
        nlp_total,
        log_module,
        gap,
        vocal_total,
        last_pre_end_start,
        asrctrl_duration,
        x_b3_traceid,
        asrctrl_inner,
        caller,
        is_pre_end,
        vpr_id,
        asrctrl_robot_skill_total,
        asr_nlp_total,
        audio_size,
        k8s_svc_name,
        k8s_env_name,
        event_time,
        ext,
        dt,
        row_number() over(partition by question_id order by event_time asc) as rnk
    from asrctrl
)
insert overwrite table cdmdwd.dwd_asr_cmd_asr_hit_domain_i_d partition(dt)
    select
        question_id,
        robot_id,
        robot_account_id,
        version,
        service_code,
        tenant_id,
        robot_type,
        rod_type,
        question_text,
        asr_domain,
        asr_third_total,
        asr_vendor,
        asr_start,
        asr_end,
        nlufwd,
        sv_agent_id,
        wake_status,
        wake_status_name,
        nlp_total,
        log_module,
        gap,
        vocal_total,
        last_pre_end_start,
        asrctrl_duration,
        x_b3_traceid,
        asrctrl_inner,
        caller,
        is_pre_end,
        vpr_id,
        asrctrl_robot_skill_total,
        asr_nlp_total,
        audio_size,
        k8s_svc_name,
        k8s_env_name,
        event_time,
        ext,
        dt
    from asrctrl_remove_duplicate
    where rnk = 1;