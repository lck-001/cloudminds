set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;
set mapreduce.job.queuename=root.users.liuhao;

with hi_intelligence_tmp as (
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
           and trim(GET_JSON_OBJECT(json_extract,'$.json_extract.rodType')) = 'hiInventionCounter'
           and trim(k8s_svc_name) in ('harix-seats-api')
    ) t where substring(event_time,0,10) >= date_sub('${s_dt_var}',2) and substring(event_time,0,10) <= '${e_dt_var}'
),
hi_intelligence as (
    select
        nvl(GET_JSON_OBJECT(json_extract,'$.json_extract.data.questionId'),'') as question_id,
        nvl(GET_JSON_OBJECT(json_extract,'$.json_extract.robotId'),'') as robot_id,
        nvl(GET_JSON_OBJECT(json_extract,'$.json_extract.rcuId'),'') as rcu_id,
        nvl(GET_JSON_OBJECT(json_extract,'$.json_extract.accountId'),'') as robot_account_id,
        nvl(GET_JSON_OBJECT(json_extract,'$.json_extract.serviceCode'),'') as service_code,
        nvl(GET_JSON_OBJECT(json_extract,'$.json_extract.tenantId'),'') as tenant_id,
        nvl(GET_JSON_OBJECT(json_extract,'$.json_extract.robotType'),'') as robot_type,
        nvl(GET_JSON_OBJECT(json_extract,'$.json_extract.rodType'),'') as rod_type,
        case when nvl(GET_JSON_OBJECT(json_extract,'$.json_extract.seat'),'') != '' then nvl(GET_JSON_OBJECT(json_extract,'$.json_extract.seat'),'')
             else nvl(GET_JSON_OBJECT(json_extract,'$.json_extract.data.userName'),'')
        end as hi_account,
        regexp_replace(nvl(GET_JSON_OBJECT(json_extract,'$.json_extract.data.eventContent'),''), '\r|\n|\t', '') as msg,
        nvl(GET_JSON_OBJECT(json_extract,'$.json_extract.data.uiSource'),'') as msg_from,
        case when trim(nvl(GET_JSON_OBJECT(json_extract,'$.json_extract.data.uiSource'),'')) = 'hiChatListConfirmSimQ' then '确认相似问题'
             when trim(nvl(GET_JSON_OBJECT(json_extract,'$.json_extract.data.uiSource'),'')) = 'hiChatListConfirmGW' then '确认问题gateway问题'
             when trim(nvl(GET_JSON_OBJECT(json_extract,'$.json_extract.data.uiSource'),'')) = 'hiChatListConfirmAiAnswer' then '确认答案'
             when trim(nvl(GET_JSON_OBJECT(json_extract,'$.json_extract.data.uiSource'),'')) = 'hiQuestionPanel' then '输入框输入问题'
             when trim(nvl(GET_JSON_OBJECT(json_extract,'$.json_extract.data.uiSource'),'')) = 'hiAnswerPanel' then '输入框输入答案'
             when trim(nvl(GET_JSON_OBJECT(json_extract,'$.json_extract.data.uiSource'),'')) = 'hiDancePanel' then '常用舞蹈'
             when trim(nvl(GET_JSON_OBJECT(json_extract,'$.json_extract.data.uiSource'),'')) = 'hiActionPanel' then '常用动作'
             when trim(nvl(GET_JSON_OBJECT(json_extract,'$.json_extract.data.uiSource'),'')) = 'hiShortcutPanel' then '自定义按钮'
             when trim(nvl(GET_JSON_OBJECT(json_extract,'$.json_extract.data.uiSource'),'')) = 'hiPhaseList' then '常用短语'
             else '未知'
        end as msg_from_name,
        case when trim(nvl(GET_JSON_OBJECT(json_extract,'$.json_extract.data.uiSource'),'')) in ('hiQuestionPanel','hiChatListConfirmSimQ','hiChatListConfirmGW') then 'question'
             when trim(nvl(GET_JSON_OBJECT(json_extract,'$.json_extract.data.uiSource'),'')) in ('hiShortcutPanel','hiPhaseList') and (GET_JSON_OBJECT(json_extract,'$.json_extract.data.isQuestion') = true or GET_JSON_OBJECT(json_extract,'$.json_extract.data.isQuestion') = 'true') then 'question'
             when trim(nvl(GET_JSON_OBJECT(json_extract,'$.json_extract.data.uiSource'),'')) in ('hiAnswerPanel','hiChatListConfirmAiAnswer') then 'answer'
             when trim(nvl(GET_JSON_OBJECT(json_extract,'$.json_extract.data.uiSource'),'')) in ('hiShortcutPanel','hiPhaseList') and  GET_JSON_OBJECT(json_extract,'$.json_extract.data.isQuestion') != true and GET_JSON_OBJECT(json_extract,'$.json_extract.data.isAction') != true then 'answer'
             when trim(nvl(GET_JSON_OBJECT(json_extract,'$.json_extract.data.uiSource'),'')) in ('hiDancePanel','hiActionPanel')  then 'action'
             when GET_JSON_OBJECT(json_extract,'$.json_extract.data.uiSource') is null and GET_JSON_OBJECT(json_extract,'$.json_extract.data.isQuestion') != true and GET_JSON_OBJECT(json_extract,'$.json_extract.data.isAction') != false then 'action'
             else '未知'
        end as msg_type,
        nvl(k8s_svc_name,'') as k8s_svc_name,
        nvl(k8s_env_name,'') as k8s_env_name,
        event_time,
        json_extract as ext,
        case when length(GET_JSON_OBJECT(json_extract,'$.json_extract.timestamp')) = 13 then from_unixtime( cast(GET_JSON_OBJECT(json_extract,'$.json_extract.timestamp')/1000 as int),'yyyy-MM-dd')
             when length(GET_JSON_OBJECT(json_extract,'$.json_extract.timestamp')) = 10 then from_unixtime( cast(GET_JSON_OBJECT(json_extract,'$.json_extract.timestamp') as int),'yyyy-MM-dd')
             else SUBSTRING(GET_JSON_OBJECT(json_extract,'$.json_extract.timestamp'),0,10)
        end as dt
    from hi_intelligence_tmp
),
-- 去重数据
hi_intelligence_remove_duplicate as (
    select
        question_id,
        robot_id,
        rcu_id,
        robot_account_id,
        service_code,
        tenant_id,
        robot_type,
        rod_type,
        hi_account,
        msg,
        msg_from,
        msg_from_name,
        msg_type,
        k8s_svc_name,
        k8s_env_name,
        event_time,
        ext,
        dt,
        row_number() over(partition by question_id,msg order by (case when hi_account != '' then 0 else 1 end),event_time asc) as rnk
    from hi_intelligence
)
insert overwrite table cdmdwd.dwd_harix_cmd_hi_intelligence_i_d partition(dt)
    select
        question_id,
        robot_id,
        rcu_id,
        robot_account_id,
        service_code,
        tenant_id,
        robot_type,
        rod_type,
        hi_account,
        msg,
        msg_from,
        msg_from_name,
        msg_type,
        k8s_svc_name,
        k8s_env_name,
        event_time,
        ext,
        dt
    from hi_intelligence_remove_duplicate
    where rnk = 1;