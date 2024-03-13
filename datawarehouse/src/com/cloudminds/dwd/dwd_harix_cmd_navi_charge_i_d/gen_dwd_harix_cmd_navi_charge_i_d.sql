set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;
set mapreduce.job.queuename=root.users.liuhao;

with etl_es as (
    select
        json_extract,
        k8s_svc_name,
        k8s_env_name,
        tdate,
        rod_type,
        event_time
    from (
        select
            json_extract,
            k8s_svc_name,
            k8s_env_name,
            tdate,
            nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.rodType')),'') as rod_type,
            case when length(GET_JSON_OBJECT(json_extract,'$.json_extract.timestamp')) = 10 then nvl(concat(from_unixtime( cast(GET_JSON_OBJECT(json_extract,'$.json_extract.timestamp') as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
                 when length(GET_JSON_OBJECT(json_extract,'$.json_extract.timestamp')) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(GET_JSON_OBJECT(json_extract,'$.json_extract.timestamp'),'T',' '),0,23),'')
                 else nvl(concat(from_unixtime( cast(GET_JSON_OBJECT(json_extract,'$.json_extract.timestamp')/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(GET_JSON_OBJECT(json_extract,'$.json_extract.timestamp'),11,3)),'')
            end as event_time
        from hari.harix_etl_es
        where tdate >= date_sub('${s_dt_var}',2) and tdate <= '${e_dt_var}'
        and nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.rodType')),'') in ('naviDeal')
        and nvl(trim(k8s_svc_name),'') in ('harix-skill-navi')
        and nvl(trim(GET_JSON_OBJECT(GET_JSON_OBJECT(GET_JSON_OBJECT(json_extract,'$.json_extract.naviData.request.param'),'$.param'),'$.status')),-99999998) in (111,112,113,114,115,116,117)
        distribute by rand()
     ) t where substring(event_time,0,10) >= date_sub('${s_dt_var}',2) and substring(event_time,0,10) <= '${e_dt_var}'
),
navi_charge as (
    select
        case when nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.rootGuid')),'') != '' then nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.rootGuid')),'')
             when nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.guid')),'') != '' then nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.guid')),'')
             when nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.naviData.request.commonReqInfo.rootGuid')),'') != '' then nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.naviData.request.commonReqInfo.rootGuid')),'')
             else nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.naviData.request.commonReqInfo.guid')),'')
        end as guid,
        case when nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.robotId')),'') != '' then nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.robotId')),'')
             else nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.naviData.request.commonReqInfo.robotId')),'')
        end as robot_id,
        case when nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.robotType')),'') != '' then nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.robotType')),'')
             else nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.naviData.request.commonReqInfo.robotType')),'')
        end as robot_type,
        case when nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.userId')),'') != '' then nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.userId')),'')
             else nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.naviData.request.commonReqInfo.userId')),'')
        end as robot_account_id,
        case when nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.serviceCode')),'') != '' then nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.serviceCode')),'')
             else nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.naviData.request.commonReqInfo.serviceCode')),'')
        end as service_code,
        case when nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.tenantId')),'') != '' then nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.tenantId')),'')
             else nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.naviData.request.commonReqInfo.tenantId')),'')
        end as tenant_id,
        case when nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.version')),'') != '' then nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.version')),'')
             else nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.naviData.request.commonReqInfo.version')),'')
        end as version,
        case when nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.seq')),'') != '' then nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.seq')),'')
             else nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.naviData.request.commonReqInfo.seq')),'')
        end as seq,
        nvl(trim(GET_JSON_OBJECT(GET_JSON_OBJECT(json_extract,'$.json_extract.naviData.request.param'),'$.action')),'') as action,
        nvl(trim(GET_JSON_OBJECT(GET_JSON_OBJECT(GET_JSON_OBJECT(json_extract,'$.json_extract.naviData.request.param'),'$.param'),'$.opId')),'') as op_id,
        nvl(trim(GET_JSON_OBJECT(GET_JSON_OBJECT(GET_JSON_OBJECT(json_extract,'$.json_extract.naviData.request.param'),'$.param'),'$.status')),-99999998) as status,
        case when nvl(trim(GET_JSON_OBJECT(GET_JSON_OBJECT(GET_JSON_OBJECT(json_extract,'$.json_extract.naviData.request.param'),'$.param'),'$.status')),-99999998) = 111 then '正在对桩'
             when nvl(trim(GET_JSON_OBJECT(GET_JSON_OBJECT(GET_JSON_OBJECT(json_extract,'$.json_extract.naviData.request.param'),'$.param'),'$.status')),-99999998) = 112 then '对桩成功，开始充电'
             when nvl(trim(GET_JSON_OBJECT(GET_JSON_OBJECT(GET_JSON_OBJECT(json_extract,'$.json_extract.naviData.request.param'),'$.param'),'$.status')),-99999998) = 113 then '下桩成功'
             when nvl(trim(GET_JSON_OBJECT(GET_JSON_OBJECT(GET_JSON_OBJECT(json_extract,'$.json_extract.naviData.request.param'),'$.param'),'$.status')),-99999998) = 114 then '找不到充电桩'
             when nvl(trim(GET_JSON_OBJECT(GET_JSON_OBJECT(GET_JSON_OBJECT(json_extract,'$.json_extract.naviData.request.param'),'$.param'),'$.status')),-99999998) = 115 then '对桩失败'
             when nvl(trim(GET_JSON_OBJECT(GET_JSON_OBJECT(GET_JSON_OBJECT(json_extract,'$.json_extract.naviData.request.param'),'$.param'),'$.status')),-99999998) = 116 then '对桩成功，但是对桩后充电器没有给机器人充电'
             when nvl(trim(GET_JSON_OBJECT(GET_JSON_OBJECT(GET_JSON_OBJECT(json_extract,'$.json_extract.naviData.request.param'),'$.param'),'$.status')),-99999998) = 117 then '下桩失败'
             else '未知'
        end as status_name,
    nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.naviData.request.context')),'') as context,
    nvl(trim(GET_JSON_OBJECT(json_extract,'$.json_extract.naviData.request.id')),'') as msg,
    rod_type,
    nvl(k8s_svc_name,'') as k8s_svc_name,
    nvl(k8s_env_name,'') as k8s_env_name,
    event_time,
    json_extract as ext,
    case when length(GET_JSON_OBJECT(json_extract,'$.json_extract.timestamp')) = 13 then from_unixtime( cast(GET_JSON_OBJECT(json_extract,'$.json_extract.timestamp')/1000 as int),'yyyy-MM-dd')
         when length(GET_JSON_OBJECT(json_extract,'$.json_extract.timestamp')) = 10 then from_unixtime( cast(GET_JSON_OBJECT(json_extract,'$.json_extract.timestamp') as int),'yyyy-MM-dd')
         else SUBSTRING(GET_JSON_OBJECT(json_extract,'$.json_extract.timestamp'),0,10)
    end as dt
    from etl_es
)

insert overwrite table cdmdwd.dwd_harix_cmd_navi_charge_i_d partition(dt)
    select
        guid,
        robot_id,
        robot_type,
        robot_account_id,
        service_code,
        tenant_id,
        version,
        seq,
        action,
        op_id,
        status,
        status_name,
        context,
        msg,
        rod_type,
        k8s_svc_name,
        k8s_env_name,
        event_time,
        ext,
        dt
    from navi_charge;
