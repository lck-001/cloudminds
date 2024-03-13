set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;
set mapreduce.job.queuename=root.users.liuhao;

with reception_send_tmp as (
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
        and trim(GET_JSON_OBJECT(json_extract,'$.json_extract.rodType')) = 'RobotMindSendQa'
        and GET_JSON_OBJECT(json_extract,'$.json_extract.robotData.request.Answer') is null
        and trim(k8s_svc_name) in ('harix-skill-robot')
    ) t where substring(event_time,0,10) >= date_sub('${s_dt_var}',2) and substring(event_time,0,10) <= '${e_dt_var}'
),
reception_send as (
    select
        case when nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.robotData.request.Question.questionId')),'') != '' then nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.robotData.request.Question.questionId')),'')
             else nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.rootGuid')),'')
        end as question_id,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.robotId')),'') as robot_id,
        nvl(GET_JSON_OBJECT(json_extract,'$.json_extract.robotData.request.Question.audio'),'') as rcu_audio,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.userId')),'') as robot_account_id,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.serviceCode')),'') as service_code,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.tenantId')),'') as tenant_id,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.robotType')),'') as robot_type,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.rodType')),'') as rod_type,
        nvl(GET_JSON_OBJECT(json_extract,'$.json_extract.robotData.request.Question.qaFlag'),'') as qa_flag,
        regexp_replace(nvl(GET_JSON_OBJECT(json_extract,'$.json_extract.robotData.request.Question.text'),''), '\r|\n|\t', '') as question_text,
        case when nvl(GET_JSON_OBJECT(GET_JSON_OBJECT(json_extract,'$.json_extract.robotData.request.Question.lang'),'$.lang'),'') != '' then nvl(GET_JSON_OBJECT(GET_JSON_OBJECT(json_extract,'$.json_extract.robotData.request.Question.lang'),'$.lang'),'')
             when nvl(regexp_extract(GET_JSON_OBJECT(json_extract,'$.json_extract.robotData.request.Question.lang'),'"lang":"(.+?)"}'),'') != '' then nvl(regexp_extract(GET_JSON_OBJECT(json_extract,'$.json_extract.robotData.request.Question.lang'),'"lang":"(.+?)"}'),'')
             else nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.robotData.request.Question.lang')),'')
        end as question_language,
        cast(nvl(GET_JSON_OBJECT(json_extract,'$.json_extract.robotData.execTime'),-99999998) as int) as send_delay,
        nvl(k8s_svc_name,'') as k8s_svc_name,
        nvl(k8s_env_name,'') as k8s_env_name,
        event_time,
        json_extract as ext,
        case when length(GET_JSON_OBJECT(json_extract,'$.json_extract.timestamp')) = 13 then from_unixtime( cast(GET_JSON_OBJECT(json_extract,'$.json_extract.timestamp')/1000 as int),'yyyy-MM-dd')
             when length(GET_JSON_OBJECT(json_extract,'$.json_extract.timestamp')) = 10 then from_unixtime( cast(GET_JSON_OBJECT(json_extract,'$.json_extract.timestamp') as int),'yyyy-MM-dd')
             else SUBSTRING(GET_JSON_OBJECT(json_extract,'$.json_extract.timestamp'),0,10)
        end as dt
    from reception_send_tmp
),
--去除重复数据   包含部分数据question_id 为下划线开始的数据在asr_send_text 存在的
reception_send_remove_duplicate as (
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
        question_language,
        send_delay,
        k8s_svc_name,
        k8s_env_name,
        event_time,
        ext,
        dt,
        row_number() over(partition by question_id order by event_time asc) as rnk
    from reception_send
)

insert overwrite table cdmdwd.dwd_harix_cmd_reception_send_txt_i_d partition(dt)
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
        question_language,
        send_delay,
        k8s_svc_name,
        k8s_env_name,
        event_time,
        ext,
        dt
    from reception_send_remove_duplicate
    where rnk = 1;