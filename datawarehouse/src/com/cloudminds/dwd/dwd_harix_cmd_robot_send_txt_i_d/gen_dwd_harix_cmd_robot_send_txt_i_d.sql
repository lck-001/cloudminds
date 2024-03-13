set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;
set mapreduce.job.queuename=root.users.liuhao;

with robot_send_tmp as (
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
        and trim(GET_JSON_OBJECT(json_extract,'$.json_extract.rodType')) = 'robotAgentMsg'
        and GET_JSON_OBJECT(GET_JSON_OBJECT(json_extract,'$.json_extract.switchData.request.params.data'),'$.answer') is null
        and GET_JSON_OBJECT(json_extract,'$.json_extract.switchData.request.msg.type') != 'aiStatus' -- 排除aistatus，后续如果还有其它类型，重构此业务过程
        and trim(k8s_svc_name) in ('harix-switch')
        --    and (GET_JSON_OBJECT(GET_JSON_OBJECT(json_extract,'$.json_extract.switchData.request.params.data'),'$.question') is not null
        --    OR GET_JSON_OBJECT(GET_JSON_OBJECT(json_extract,'$.json_extract.switchData.request.params.data'),'$.context') is null
        --    )
    ) t where substring(event_time,0,10) >= date_sub('${s_dt_var}',2) and substring(event_time,0,10) <= '${e_dt_var}'
),
robot_send as (
    select
        case when nvl(GET_JSON_OBJECT(GET_JSON_OBJECT(json_extract,'$.json_extract.switchData.request.params.data'),'$.question.questionId'),'') != '' then nvl(GET_JSON_OBJECT(GET_JSON_OBJECT(json_extract,'$.json_extract.switchData.request.params.data'),'$.question.questionId'),'')
             else nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.rootGuid')),'')
        end as question_id,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.robotId')),'') as robot_id,
        nvl(GET_JSON_OBJECT(GET_JSON_OBJECT(json_extract,'$.json_extract.switchData.request.params.data'),'$.question.audio'),'') as rcu_audio,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.userId')),'') as robot_account_id,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.serviceCode')),'') as service_code,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.tenantId')),'') as tenant_id,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.robotType')),'') as robot_type,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.rodType')),'') as rod_type,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.switchId')),'') as harix_switch_id,
        nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.rcuId')),'') as rcu_id,
        nvl(GET_JSON_OBJECT(json_extract,'$.json_extract.switchData.request.msg.params.typeUrl'),'') as type_url,
        nvl(GET_JSON_OBJECT(GET_JSON_OBJECT(json_extract,'$.json_extract.switchData.request.params.data'),'$.question.qaFlag'),'') as qa_flag,
        regexp_replace(nvl(GET_JSON_OBJECT(GET_JSON_OBJECT(json_extract,'$.json_extract.switchData.request.params.data'),'$.question.text'),''), '\r|\n|\t', '') as question_text,
        case when GET_JSON_OBJECT(GET_JSON_OBJECT(json_extract,'$.json_extract.switchData.request.params.data'),'$.question.lang.lang') is not null then nvl(GET_JSON_OBJECT(GET_JSON_OBJECT(json_extract,'$.json_extract.switchData.request.params.data'),'$.question.lang.lang'),'')
             when nvl(regexp_extract(GET_JSON_OBJECT(GET_JSON_OBJECT(json_extract,'$.json_extract.switchData.request.params.data'),'$.question.lang'),'"lang":(.+?),'),'') != '' then nvl(regexp_extract(GET_JSON_OBJECT(GET_JSON_OBJECT(json_extract,'$.json_extract.switchData.request.params.data'),'$.question.lang'),'"lang":(.+?),'),'')
             else nvl(GET_JSON_OBJECT(GET_JSON_OBJECT(json_extract,'$.json_extract.switchData.request.params.data'),'$.question.lang'),'')
        end as question_language,
        nvl(GET_JSON_OBJECT(json_extract,'$.json_extract.switchData.request.msg.src'),'') as msg_from,
        nvl(GET_JSON_OBJECT(json_extract,'$.json_extract.switchData.request.msg.from'),'') as msg_send_name,
        nvl(GET_JSON_OBJECT(json_extract,'$.json_extract.switchData.request.msg.id'),'') as msg_id,
        nvl(GET_JSON_OBJECT(json_extract,'$.json_extract.switchData.request.msg.to'),'') as msg_receive_name,
        nvl(GET_JSON_OBJECT(json_extract,'$.json_extract.switchData.request.msg.type'),'') as msg_type,
        nvl(GET_JSON_OBJECT(json_extract,'$.json_extract.switchData.request.msg.dest'),'') as msg_dest,
        nvl(k8s_svc_name,'') as k8s_svc_name,
        nvl(k8s_env_name,'') as k8s_env_name,
        event_time,
        json_extract as ext,
        case when length(GET_JSON_OBJECT(json_extract,'$.json_extract.timestamp')) = 13 then from_unixtime( cast(GET_JSON_OBJECT(json_extract,'$.json_extract.timestamp')/1000 as int),'yyyy-MM-dd')
             when length(GET_JSON_OBJECT(json_extract,'$.json_extract.timestamp')) = 10 then from_unixtime( cast(GET_JSON_OBJECT(json_extract,'$.json_extract.timestamp') as int),'yyyy-MM-dd')
             else SUBSTRING(GET_JSON_OBJECT(json_extract,'$.json_extract.timestamp'),0,10)
        end as dt
    from robot_send_tmp
),
-- 去重数据
robot_send_remove_duplicate as (
    select
        question_id,
        robot_id,
        rcu_audio,
        robot_account_id,
        service_code,
        tenant_id,
        robot_type,
        rod_type,
        harix_switch_id,
        rcu_id,
        type_url,
        qa_flag,
        question_text,
        question_language,
        msg_from,
        msg_send_name,
        msg_id,
        msg_receive_name,
        msg_type,
        msg_dest,
        k8s_svc_name,
        k8s_env_name,
        event_time,
        ext,
        dt,
        row_number() over(partition by question_id order by (case when rcu_audio != '' then 0 else 1 end),event_time asc) as rnk
    from robot_send
)
insert overwrite table cdmdwd.dwd_harix_cmd_robot_send_txt_i_d partition(dt)
    select
        question_id,
        robot_id,
        rcu_audio,
        robot_account_id,
        service_code,
        tenant_id,
        robot_type,
        rod_type,
        harix_switch_id,
        rcu_id,
        type_url,
        qa_flag,
        question_text,
        question_language,
        msg_from,
        msg_send_name,
        msg_id,
        msg_receive_name,
        msg_type,
        msg_dest,
        k8s_svc_name,
        k8s_env_name,
        event_time,
        ext,
        dt
    from robot_send_remove_duplicate
    where rnk = 1;