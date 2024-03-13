set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;
set mapreduce.job.queuename=root.users.liuhao;

with asr_send_tmp as (
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
        and trim(GET_JSON_OBJECT(json_extract,'$.json_extract.rodType')) = 'reception'
        and GET_JSON_OBJECT(json_extract,'$.json_extract.receptionData.textResponse') is null
        and trim(k8s_svc_name) in ('harix-skill-reception')
        --    and GET_JSON_OBJECT(json_extract,'$.json_extract.receptionData.request.imageInfo') is null  -- 排除人脸识别数据
        and GET_JSON_OBJECT(json_extract,'$.json_extract.receptionData.request.body') is not null  -- 排除人脸识别数据
    ) t where substring(event_time,0,10) >= date_sub('${s_dt_var}',2) and substring(event_time,0,10) <= '${e_dt_var}'
),
asr_send as (
    select
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.rootGuid')),'') as question_id,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.robotId')),'') as robot_id,
        nvl(GET_JSON_OBJECT(json_extract,'$.json_extract.receptionData.request.body.question.audio'),'') as rcu_audio,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.userId')),'') as robot_account_id,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.serviceCode')),'') as service_code,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.tenantId')),'') as tenant_id,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.robotType')),'') as robot_type,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.rodType')),'') as rod_type,
        nvl(GET_JSON_OBJECT(json_extract,'$.json_extract.receptionData.request.body.question.qaFlag'),'') as qa_flag,
        regexp_replace(nvl(GET_JSON_OBJECT(json_extract,'$.json_extract.receptionData.request.body.question.text'),''), '\r|\n|\t', '') as question_text,
        nvl(GET_JSON_OBJECT(json_extract,'$.json_extract.receptionData.cache.hiStatus'),'') as hi_status,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.receptionData.cache.lang')),'') as `language`,
        nvl(GET_JSON_OBJECT(json_extract,'$.json_extract.receptionData.cache.latitude'),'') as latitude,
        nvl(GET_JSON_OBJECT(json_extract,'$.json_extract.receptionData.cache.longitude'),'') as longitude,
        nvl(GET_JSON_OBJECT(json_extract,'$.json_extract.receptionData.receptionLog.sendQuestion.executeDelay'),-99999998) as execute_delay,
        nvl(GET_JSON_OBJECT(json_extract,'$.json_extract.receptionData.receptionLog.queueDelay'),-99999998) as queue_delay,
        nvl(GET_JSON_OBJECT(json_extract,'$.json_extract.receptionData.receptionLog.totalDelay'),-99999998) as total_delay,
        nvl(k8s_svc_name,'') as k8s_svc_name,
        nvl(k8s_env_name,'') as k8s_env_name,
        event_time,
        json_extract as ext,
        case when length(GET_JSON_OBJECT(json_extract,'$.json_extract.timestamp')) = 13 then from_unixtime( cast(GET_JSON_OBJECT(json_extract,'$.json_extract.timestamp')/1000 as int),'yyyy-MM-dd')
             when length(GET_JSON_OBJECT(json_extract,'$.json_extract.timestamp')) = 10 then from_unixtime( cast(GET_JSON_OBJECT(json_extract,'$.json_extract.timestamp') as int),'yyyy-MM-dd')
             else SUBSTRING(GET_JSON_OBJECT(json_extract,'$.json_extract.timestamp'),0,10)
        end as dt
    from asr_send_tmp
),
-- 去重数据
asr_send_remove_duplicate as (
    select
        question_id,
        robot_id,
        rcu_audio,
        robot_account_id,
        service_code,
        tenant_id,
        robot_type,
        rod_type,
        qa_flag,
        question_text,
        hi_status,
        `language`,
        latitude,
        longitude,
        execute_delay,
        queue_delay,
        total_delay,
        k8s_svc_name,
        k8s_env_name,
        event_time,
        ext,
        dt,
        row_number() over(partition by question_id order by (case when rcu_audio != '' then 0 else 1 end),event_time asc) as rnk
    from asr_send
)
insert overwrite table cdmdwd.dwd_harix_cmd_asr_send_txt_i_d partition(dt)
    select
        question_id,
        robot_id,
        rcu_audio,
        robot_account_id,
        service_code,
        tenant_id,
        robot_type,
        rod_type,
        qa_flag,
        question_text,
        hi_status,
        `language`,
        latitude,
        longitude,
        execute_delay,
        queue_delay,
        total_delay,
        k8s_svc_name,
        k8s_env_name,
        event_time,
        ext,
        dt
    from asr_send_remove_duplicate
    where rnk = 1;
