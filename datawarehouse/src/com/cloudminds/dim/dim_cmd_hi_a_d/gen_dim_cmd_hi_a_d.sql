SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.merge.mapfiles = true;
SET hive.merge.mapredfiles = true;
SET hive.exec.max.dynamic.partitions = 60000;
set mapreduce.job.queuename=root.users.liuhao;

with union_hi as (
    select
        id,
        tenant_code,
        user_name,
        login_id,
        email,
        phone_code,
        `type`,
        status,
        operator_id,
        seats_group_id,
        sync_batch_no,
        k8s_env_name,
        create_time,
        update_time
    from cdmods.ods_hari_db_c0004_t_seats_s_d
    where dt = '${e_dt_var}'
    union all
    select
        id,
        tenant_code,
        user_name,
        login_id,
        email,
        phone_code,
        `type`,
        status,
        operator_id,
        seats_group_id,
        sync_batch_no,
        k8s_env_name,
        create_time,
        update_time
    from cdmods.ods_hari_db_c0009_t_seats_s_d
    where dt = '${e_dt_var}'
)
insert overwrite table cdmdim.dim_cmd_hi_a_d
    select
        nvl(id,'') as hi_id,
        nvl(login_id,'') as hi_account,
        nvl(tenant_code,'') as tenant_id,
        nvl(user_name,'') as hi_name,
        nvl(email,'') as email,
        nvl(phone_code,'') as phone,
        cast (nvl(`type`,-99999998) as int) as load_type,
        case when `type` = 0 or `type` = '0' then '手工录入'
             when `type` = 1 or `type` = '1' then '文件导入'
             when `type` = 2 or `type` = '2' then 'ad导入'
             when `type` = 3 or `type` = '3' then 'ad同步'
             else '未知'
        end as load_type_name,
        nvl(status,-99999998) as status,
        case when status = -1 or status = '-1' then '删除'
             when status = 0 or status = '0' then '停用'
             when status = 1 or status = '1' then '启用'
             when status = 2 or status = '2' then '无效'
             else '未知'
        end as status_name,
        nvl(operator_id,'') as operator_id,
        nvl(seats_group_id,'') as hi_group,
        nvl(sync_batch_no,'') as sync_batch_no,
        nvl(k8s_env_name,'') as k8s_env_name,
        case when nvl(create_time,'') != '' then concat(create_time,'.000')
             when nvl(update_time,'') != '' then concat(update_time,'.000')
             else concat(from_utc_timestamp(current_timestamp,'GMT+8'),'.000')
        end as create_time,
        case when nvl(update_time,'') != '' then concat(update_time,'.000')
             when nvl(create_time,'') != '' then concat(create_time,'.000')
             else concat(from_utc_timestamp(current_timestamp,'GMT+8'),'.000')
        end as update_time
    from union_hi;