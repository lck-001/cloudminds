set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;
set mapreduce.job.queuename=root.users.liuhao;

with roc_robot_account as (
    SELECT 
        nvl(user_code, '') as robot_account_id,
        nvl(user_name, '') as robot_account_name,
        nvl(email, '') as email,
        nvl(phone_code, '') as phone,
        nvl(`status`, -99999998) as `status`,
        case when status = -1 then '删除'
             when status = 0 then '正常'
             when status = 1 then '停用'
             else '未知'
        end as status_name,
        case when length(create_time) = 10 then nvl(concat(from_unixtime( cast(create_time as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
             when length(create_time) = 19 then nvl(concat(create_time,'.000'),'')
             when length(create_time) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(create_time,'T',' '),0,23),'')
             else nvl(concat(from_unixtime( cast(create_time/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(create_time,11,3)),'')
        end as create_time,
        case when length(update_time) = 10 then nvl(concat(from_unixtime( cast(update_time as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
             when length(update_time) = 19 then nvl(concat(update_time,'.000'),'')
             when length(update_time) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(update_time,'T',' '),0,23),'')
             else nvl(concat(from_unixtime( cast(update_time/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(update_time,11,3)),'')
        end as update_time,
        nvl(privacy_type, -99999998) as privacy_type,
        case when privacy_type = 1 then '公有'
             when privacy_type = 2 then '私有'
             else '未知'
        end as privacy_type_name,
        nvl(hi_service, -99999998) as is_hi_service,
        nvl(privacy_enhance, -99999998) as is_privacy_enhance,
        nvl(auto_takeover, -99999998) as is_auto_takeover,
        nvl(app_id, '') as app_id,
        nvl(video_mode, '') as video_mode,
        nvl(phone_number_prefix, '') as phone_number_prefix,
        nvl(tenant_code, '') as tenant_id,
        nvl(operator_id, '') as operator_id,
        nvl(ross_credential_id, '') as ross_credential_id,
        k8s_env_name,
        nvl(concat(update_time,'.000'), '') as start_time,
        '' as end_time
    FROM cdmods.ods_roc_db_c0002_t_user_s_d where dt = '${e_dt_var}'
    union all
    SELECT
        nvl(user_code, '') as robot_account_id,
        nvl(user_name, '') as robot_account_name,
        nvl(email, '') as email,
        nvl(phone_code, '') as phone,
        nvl(`status`, -99999998) as `status`,
        case when status = -1 then '删除'
             when status = 0 then '正常'
             when status = 1 then '停用'
             else '未知'
            end as status_name,
        case when length(create_time) = 10 then nvl(concat(from_unixtime( cast(create_time as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
             when length(create_time) = 19 then nvl(concat(create_time,'.000'),'')
             when length(create_time) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(create_time,'T',' '),0,23),'')
             else nvl(concat(from_unixtime( cast(create_time/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(create_time,11,3)),'')
            end as create_time,
        case when length(update_time) = 10 then nvl(concat(from_unixtime( cast(update_time as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
             when length(update_time) = 19 then nvl(concat(update_time,'.000'),'')
             when length(update_time) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(update_time,'T',' '),0,23),'')
             else nvl(concat(from_unixtime( cast(update_time/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(update_time,11,3)),'')
            end as update_time,
        nvl(privacy_type, -99999998) as privacy_type,
        case when privacy_type = 1 then '公有'
             when privacy_type = 2 then '私有'
             else '未知'
            end as privacy_type_name,
        nvl(hi_service, -99999998) as is_hi_service,
        nvl(privacy_enhance, -99999998) as is_privacy_enhance,
        nvl(auto_takeover, -99999998) as is_auto_takeover,
        nvl(app_id, '') as app_id,
        nvl(video_mode, '') as video_mode,
        nvl(phone_number_prefix, '') as phone_number_prefix,
        nvl(tenant_code, '') as tenant_id,
        nvl(operator_id, '') as operator_id,
        nvl(ross_credential_id, '') as ross_credential_id,
        k8s_env_name,
        nvl(concat(update_time,'.000'), '') as start_time,
        '' as end_time
    FROM cdmods.ods_roc_db_c0008_t_user_s_d 
    where 
        dt = '${e_dt_var}' 
    -- 251暂时过滤掉测试数据
    AND 
        tenant_code != '251_auto'
),
union_robot_account as (
    select
        robot_account_id,
        robot_account_name,
        email,
        phone,
        status,
        status_name,
        privacy_type,
        privacy_type_name,
        is_hi_service,
        is_privacy_enhance,
        is_auto_takeover,
        app_id,
        video_mode,
        phone_number_prefix,
        tenant_id,
        operator_id,
        ross_credential_id,
        start_time,
        end_time,
        k8s_env_name,
        create_time,
        update_time,
        row_number() over(partition by robot_account_id,robot_account_name,email,phone,status,status_name,privacy_type,privacy_type_name,is_hi_service,is_privacy_enhance,is_auto_takeover,app_id,video_mode,phone_number_prefix,tenant_id,operator_id,ross_credential_id,k8s_env_name,create_time,update_time order by update_time asc) as rnk
    from (
         select
             robot_account_id,
             robot_account_name,
             email,
             phone,
             status,
             status_name,
             privacy_type,
             privacy_type_name,
             is_hi_service,
             is_privacy_enhance,
             is_auto_takeover,
             app_id,
             video_mode,
             phone_number_prefix,
             tenant_id,
             operator_id,
             ross_credential_id,
             start_time,
             end_time,
             k8s_env_name,
             create_time,
             update_time
         from roc_robot_account
         union
         select
             robot_account_id,
             robot_account_name,
             email,
             phone,
             status,
             status_name,
             privacy_type,
             privacy_type_name,
             is_hi_service,
             is_privacy_enhance,
             is_auto_takeover,
             app_id,
             video_mode,
             phone_number_prefix,
             tenant_id,
             operator_id,
             ross_credential_id,
             start_time,
             end_time,
             k8s_env_name,
             create_time,
             update_time
         from cdmdim.dim_umd_robot_account_sh_d where dt = date_sub('${e_dt_var}', 1)
    ) t
),
result_table as (
    select
        robot_account_id,
        robot_account_name,
        email,
        phone,
        status,
        status_name,
        privacy_type,
        privacy_type_name,
        is_hi_service,
        is_privacy_enhance,
        is_auto_takeover,
        app_id,
        video_mode,
        phone_number_prefix,
        tenant_id,
        operator_id,
        ross_credential_id,
        update_time as start_time,
        CASE WHEN tmp_end_time IS NOT NULL AND tmp_end_time != '' THEN concat(from_unixtime(cast((cast(unix_timestamp(substring(tmp_end_time,0,19),'yyyy-MM-dd HH:mm:ss') AS bigint)*1000 + cast(substring(tmp_end_time,21,23) AS bigint) - 1)/1000 as bigint),'yyyy-MM-dd HH:mm:ss'),'.',SUBSTRING((cast(unix_timestamp(substring(tmp_end_time,0,19),'yyyy-MM-dd HH:mm:ss') AS bigint)*1000 + cast(substring(tmp_end_time,21,23) AS bigint) - 1),11,13))
             ELSE '9999-12-31 23:59:59.999'
        END AS end_time,
        k8s_env_name,
        create_time,
        update_time,
        row_number() over (partition by robot_account_id, tenant_id order by create_time, update_time asc) as r -- 这里排序目的为了让第一条记录的有效时间取create_time,往后的记录取update_time
    from (
         SELECT
             robot_account_id,
             robot_account_name,
             email,
             phone,
             status,
             status_name,
             privacy_type,
             privacy_type_name,
             is_hi_service,
             is_privacy_enhance,
             is_auto_takeover,
             app_id,
             video_mode,
             phone_number_prefix,
             tenant_id,
             operator_id,
             ross_credential_id,
             start_time,
             end_time,
             k8s_env_name,
             create_time,
             update_time,
             lead(t.update_time,1,null) over (PARTITION by t.tenant_id, t.robot_account_id, t.k8s_env_name order by t.update_time asc) as tmp_end_time
         FROM (
              select
                  robot_account_id,
                  robot_account_name,
                  email,
                  phone,
                  status,
                  status_name,
                  privacy_type,
                  privacy_type_name,
                  is_hi_service,
                  is_privacy_enhance,
                  is_auto_takeover,
                  app_id,
                  video_mode,
                  phone_number_prefix,
                  tenant_id,
                  operator_id,
                  ross_credential_id,
                  start_time,
                  end_time,
                  k8s_env_name,
                  create_time,
                  update_time
              from union_robot_account
              where rnk = 1
          ) t
    ) x
),
modify_result_table as (
    select
        robot_account_id,
        robot_account_name,
        email,
        phone,
        status,
        status_name,
        privacy_type,
        privacy_type_name,
        is_hi_service,
        is_privacy_enhance,
        is_auto_takeover,
        app_id,
        video_mode,
        phone_number_prefix,
        tenant_id,
        operator_id,
        ross_credential_id,
        case when r = 1 then create_time
             else start_time
        end as start_time,
        end_time,
        k8s_env_name,
        create_time,
        update_time
    from result_table
)
insert overwrite table cdmdim.dim_umd_robot_account_sh_d partition(dt)
    SELECT
        robot_account_id,
        robot_account_name,
        email,
        phone,
        status,
        status_name,
        privacy_type,
        privacy_type_name,
        is_hi_service,
        is_privacy_enhance,
        is_auto_takeover,
        app_id,
        video_mode,
        phone_number_prefix,
        tenant_id,
        operator_id,
        ross_credential_id,
        start_time,
        end_time,
        k8s_env_name,
        create_time,
        update_time,
        '${e_dt_var}' as dt
    FROM modify_result_table;