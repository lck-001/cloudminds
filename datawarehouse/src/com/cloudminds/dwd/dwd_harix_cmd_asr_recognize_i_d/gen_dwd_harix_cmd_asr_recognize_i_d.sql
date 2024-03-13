set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;
set mapreduce.input.fileinputformat.input.dir.recursive=true;
set  hive.exec.max.dynamic.partitions.pernode=500;
set hive.merge.smallfiles.avgsize=128000000;
set hive.merge.size.per.task=128000000;
set mapreduce.job.queuename=root.users.liuhao;

with asr_recognize_tmp as (
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
         and trim(GET_JSON_OBJECT(json_extract,'$.json_extract.rodType')) = 'asrRecognize'
         and trim(k8s_svc_name) in ('smartvoice-asrctrl','harix-skill-asr','asr-hub')
    ) t where substring(event_time,0,10) >= date_sub('${s_dt_var}',2) and substring(event_time,0,10) <= '${e_dt_var}'
),
asr_recognize as (
    select
        case when nvl(GET_JSON_OBJECT(json_extract,'$.json_extract.asrData.response.question_id'),'') != '' then nvl(GET_JSON_OBJECT(json_extract,'$.json_extract.asrData.response.question_id'),'')
             else nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.rootGuid')),'')
        end as question_id,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.robotId')),'') as robot_id,
        nvl(GET_JSON_OBJECT(json_extract,'$.json_extract.asrData.request.fileUrl'),'') as rcu_audio,
        case when nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.asrData.response.isNoise')),'') = true or nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.asrData.response.isNoise')),'') = 'true' then 1
             else 0
        end as is_noise,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.asrData.request.userId')),'') as robot_account_id,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.asrData.request.version')),'') as version,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.asrData.request.sid')),'') as sid,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.asrData.request.serviceCode')),'') as service_code,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.asrData.request.tenantId')),'') as tenant_id,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.asrData.request.appType')),'') as app_type,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.asrData.request.robotType')),'') as robot_type,
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

        nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.asrData.detail.vprId')),'') as vpr_id,
        regexp_replace(nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.asrData.detail.recognizedText')),''), '\r|\n|\t', '') as question_text,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.asrData.detail.wavLength')),-99999998) as audio_length,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.asrData.detail.vendorDelay')),-99999998) as asr_vendor_delay,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.asrData.detail.rccDelay')),-99999998) as rcc_delay,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.asrData.detail.forwardDelay')),-99999998) as forward_delay,

        nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.asrData.request.localParams.AgentId')),'') as sv_agent_id,
        nvl(k8s_svc_name,'') as k8s_svc_name,
        nvl(k8s_env_name,'') as k8s_env_name,
        event_time,
        json_extract as ext,
        case when length(GET_JSON_OBJECT(json_extract,'$.json_extract.timestamp')) = 13 then from_unixtime( cast(GET_JSON_OBJECT(json_extract,'$.json_extract.timestamp')/1000 as int),'yyyy-MM-dd')
             when length(GET_JSON_OBJECT(json_extract,'$.json_extract.timestamp')) = 10 then from_unixtime( cast(GET_JSON_OBJECT(json_extract,'$.json_extract.timestamp') as int),'yyyy-MM-dd')
             else SUBSTRING(GET_JSON_OBJECT(json_extract,'$.json_extract.timestamp'),0,10)
        end as dt
    from asr_recognize_tmp
),
-- 对数据去重，如果returnTrace 为 true ，流式处理会产出多个噪音 noise 事件
-- 去重逻辑:选取非noise且rcu_audio不为空，事件时间最小的那条
asr_recognize_remove_duplicate as (
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
        vpr_id,
        question_text,
        audio_length,
        asr_vendor_delay,
        rcc_delay,
        forward_delay,
        sv_agent_id,
        k8s_svc_name,
        k8s_env_name,
        event_time,
        ext,
        dt,
        row_number() over(partition by question_id order by (case when rcu_audio != '' then 0 else 1 end),event_time asc) as rnk
    from asr_recognize
)

insert overwrite table cdmdwd.dwd_harix_cmd_asr_recognize_i_d partition(dt)
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
        vpr_id,
        question_text,
        audio_length,
        asr_vendor_delay,
        rcc_delay,
        forward_delay,
        sv_agent_id,
        k8s_svc_name,
        k8s_env_name,
        event_time,
        ext,
        dt
    from asr_recognize_remove_duplicate
    where rnk = 1;