set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;
set mapreduce.input.fileinputformat.input.dir.recursive=true;
set mapreduce.job.queuename=root.users.liuhao;

-- 提取cdm_sv.ods_hitlog_d rod_type = 'voiceClipCounter' or rod_type = 'asrCounter'
with rcu_record_tmp as (
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
            case when length(GET_JSON_OBJECT(json_extract,'$.json_extract.timestamp')) = 10 then nvl(concat(from_unixtime( cast(GET_JSON_OBJECT(json_extract,'$.json_extract.timestamp') as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
                 when length(GET_JSON_OBJECT(json_extract,'$.json_extract.timestamp')) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(GET_JSON_OBJECT(json_extract,'$.json_extract.timestamp'),'T',' '),0,23),'')
                 else nvl(concat(from_unixtime( cast(GET_JSON_OBJECT(json_extract,'$.json_extract.timestamp')/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(GET_JSON_OBJECT(json_extract,'$.json_extract.timestamp'),11,3)),'')
            end as event_time
        from hari.harix_etl_es
        where tdate >= date_sub('${s_dt_var}',2) and tdate <= '${e_dt_var}'
        and trim(GET_JSON_OBJECT(json_extract,'$.json_extract.rodType')) in ('voiceClipCounter','asrCounter')
        and trim(k8s_svc_name) in ('smartvoice-asrctrl','harix-skill-asr','asr-hub')
        distribute by rand()
    ) t where substring(event_time,0,10) >= date_sub('${s_dt_var}',2) and substring(event_time,0,10) <= '${e_dt_var}'
),
-- 对数据清洗
rcu_record as (
    select
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.rootGuid')),'') as question_id,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.robotId')),'') as robot_id,
        nvl(GET_JSON_OBJECT(json_extract,'$.json_extract.asrData.request.fileUrl'),'') as rcu_audio,
        case when nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.asrData.response.isNoise')),'') = true or nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.asrData.response.isNoise')),'') = 'true' then 1
             when nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.receptionData.response.isNoise')),'') = true or nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.receptionData.response.isNoise')),'') = 'true' then 1
             else 0
        end as is_noise,
        case when nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.asrData.request.userId')),'') != '' then nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.asrData.request.userId')),'')
             else nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.userId')),'')
        end as robot_account_id,
        case when nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.asrData.request.version')),'') != '' then nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.asrData.request.version')),'')
             else nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.version')),'')
        end as version,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.asrData.request.sid')),'') as sid,
        case when nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.asrData.request.serviceCode')),'') != '' then nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.asrData.request.serviceCode')),'')
             else nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.serviceCode')),'')
        end as service_code,
        case when nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.asrData.request.tenantId')),'') != '' then nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.asrData.request.tenantId')),'')
             else nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.tenantId')),'')
        end as tenant_id,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.asrData.request.appType')),'') as app_type,
        case when nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.asrData.request.robotType')),'') != '' then nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.asrData.request.robotType')),'')
             else nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.robotType')),'')
        end as robot_type,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.rodType')),'') as rod_type,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.asrData.request.type')),'') as audio_record_type,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.asrData.request.qaFlag')),'') as qa_flag,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.asrData.request.data.duration')),-99999998) as duration,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.asrData.request.data.dialect')),'') as dialect,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.asrData.request.data.rate')),'') as audio_sample_rate,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.asrData.request.data.vendor')),'') as asr_vendor,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.asrData.request.data.format')),'') as format,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.asrData.request.data.channel')),-99999998) as channel,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.asrData.request.data.language')),'') as language,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.asrData.request.localParams.AgentId')),'') as sv_agent_id,
        nvl(k8s_svc_name,'') as k8s_svc_name,
        nvl(k8s_env_name,'') as k8s_env_name,
        event_time,
        json_extract as ext,
        case when length(GET_JSON_OBJECT(json_extract,'$.json_extract.timestamp')) = 13 then from_unixtime( cast(GET_JSON_OBJECT(json_extract,'$.json_extract.timestamp')/1000 as int),'yyyy-MM-dd')
             when length(GET_JSON_OBJECT(json_extract,'$.json_extract.timestamp')) = 10 then from_unixtime( cast(GET_JSON_OBJECT(json_extract,'$.json_extract.timestamp') as int),'yyyy-MM-dd')
             else SUBSTRING(GET_JSON_OBJECT(json_extract,'$.json_extract.timestamp'),0,10)
        end as dt
    from rcu_record_tmp
),
-- 对数据去重，目前存在asrCounter 和 voiceClipCounter，三种情况
-- 1.同时存在asrCounter和voiceClipCounter数据一样 2.存在asrCounter，不存在voiceClipCounter数据量较大 3.不存在asrCounter，存在voiceClipCounter 数据量较少
-- 去重规则，每一个question_id 尽量保留rcu_audio 值存在的那条最早的记录(事件时间最小的)，若不存在rcu_audio 则保留question_id中最早的记录(事件时间最小的)
rcu_record_remove_duplicate as (
    select
        question_id,
        robot_id,
        rcu_audio,
        is_noise,
        robot_account_id,
        version,
        sid,
        service_code,
        tenant_id,
        app_type,
        robot_type,
        rod_type,
        audio_record_type,
        qa_flag,
        duration,
        dialect,
        audio_sample_rate,
        asr_vendor,
        format,
        channel,
        language,
        sv_agent_id,
        k8s_svc_name,
        k8s_env_name,
        event_time,
        ext,
        dt,
        row_number() over(partition by question_id order by (case when rcu_audio != '' then 0 else 1 end),event_time asc) as rnk
    from rcu_record
)

insert overwrite table cdmdwd.dwd_harix_cmd_rcu_record_i_d partition(dt)
    select
        question_id,
        robot_id,
        rcu_audio,
        is_noise,
        robot_account_id,
        version,
        sid,
        service_code,
        tenant_id,
        app_type,
        robot_type,
        rod_type,
        audio_record_type,
        qa_flag,
        duration,
        dialect,
        audio_sample_rate,
        asr_vendor,
        format,
        channel,
        language,
        sv_agent_id,
        k8s_svc_name,
        k8s_env_name,
        event_time,
        ext,
        dt
    from rcu_record_remove_duplicate
    where rnk = 1;
