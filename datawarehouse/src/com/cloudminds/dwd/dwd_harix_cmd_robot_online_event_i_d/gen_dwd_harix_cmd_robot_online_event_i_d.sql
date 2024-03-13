set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;
set mapreduce.job.queuename=root.prod;

with robot_online_event_tmp as (
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
         where tdate >= '${s_dt_var}' and tdate <= '${e_dt_var}'
           and trim(GET_JSON_OBJECT(json_extract,'$.json_extract.rodType')) in ('robotOnlineTick60','robotConnect','robotAbnormalDisconnect')
           and trim(k8s_svc_name) in ('harix-switch')
    ) t where substring(event_time,0,10) >= '${s_dt_var}' and substring(event_time,0,10) <= '${e_dt_var}'
),
robot_online_event as (
    select
        nvl(GET_JSON_OBJECT(json_extract,'$.json_extract.serviceCode'),'') as service_code,
        nvl(GET_JSON_OBJECT(json_extract,'$.json_extract.switchId'),'') as switch_id,
        nvl(GET_JSON_OBJECT(json_extract,'$.json_extract.switchData'),'') as switch_data,
        nvl(GET_JSON_OBJECT(json_extract,'$.json_extract.errMsg'),'') as err_msg,
        nvl(GET_JSON_OBJECT(json_extract,'$.json_extract.errCode'),-99999998) as err_code,
        nvl(GET_JSON_OBJECT(json_extract,'$.json_extract.robotId'),'') as robot_id,
        nvl(GET_JSON_OBJECT(json_extract,'$.json_extract.rcuId'),'') as rcu_id,
        nvl(GET_JSON_OBJECT(json_extract,'$.json_extract.userId'),'') as robot_account_id,
        nvl(GET_JSON_OBJECT(json_extract,'$.json_extract.version'),'') as version,
        nvl(GET_JSON_OBJECT(json_extract,'$.json_extract.sid'),'') as sid,
        nvl(GET_JSON_OBJECT(json_extract,'$.json_extract.tenantId'),'') as tenant_id,
        nvl(GET_JSON_OBJECT(json_extract,'$.json_extract.robotType'),'') as robot_type_inner_name,
        nvl(GET_JSON_OBJECT(json_extract,'$.json_extract.rodType'),'') as rod_type,
        nvl(k8s_svc_name,'') as k8s_svc_name,
        nvl(k8s_env_name,'') as k8s_env_name,
        event_time,
        json_extract as ext,
        case when length(GET_JSON_OBJECT(json_extract,'$.json_extract.timestamp')) = 13 then from_unixtime( cast(GET_JSON_OBJECT(json_extract,'$.json_extract.timestamp')/1000 as int),'yyyy-MM-dd')
             when length(GET_JSON_OBJECT(json_extract,'$.json_extract.timestamp')) = 10 then from_unixtime( cast(GET_JSON_OBJECT(json_extract,'$.json_extract.timestamp') as int),'yyyy-MM-dd')
             else SUBSTRING(GET_JSON_OBJECT(json_extract,'$.json_extract.timestamp'),0,10)
        end as dt
    from robot_online_event_tmp
),
-- 去重数据
robot_online_event_remove_duplicate as (
    select
        service_code,
        switch_id,
        switch_data,
        err_msg,
        err_code,
        robot_id,
        rcu_id,
        robot_account_id,
        version,
        sid,
        tenant_id,
        robot_type_inner_name,
        rod_type,
        k8s_svc_name,
        k8s_env_name,
        event_time,
        ext,
        dt,
        row_number() over(partition by ext order by event_time asc) as rnk
    from robot_online_event
)
insert overwrite table cdmdwd.dwd_harix_cmd_robot_online_event_i_d partition(dt)
    select
        service_code,
        switch_id,
        switch_data,
        err_msg,
        err_code,
        robot_id,
        rcu_id,
        robot_account_id,
        version,
        sid,
        tenant_id,
        robot_type_inner_name,
        rod_type,
        event_time,
        k8s_svc_name,
        k8s_env_name,
        ext,
        dt
    from robot_online_event_remove_duplicate
    where rnk = 1;