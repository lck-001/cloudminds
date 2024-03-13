set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;
set mapreduce.job.queuename=root.users.liuhao;

with crio_asset_record as (
    select
        cast(id as string) as asset_record_id,
        nvl(asset_code,'') as asset_code,
        nvl(robot_code,'') as robot_id,
        nvl(action,-99999998) as action_type,
        case when action = 1 then '交付'
             when action = 2 then '内部领用'
             when action = 3 then '报障'
             when action = 4 then '维修'
             when action = 5 then '回库'
             when action = 6 then '入库'
             when action = 7 then '调拨'
             else '未知'
        end as action_type_name,
        nvl(tenant_code,'') as tenant_id,
        nvl(customer_code,'') as customer_id,
        nvl(order_code,'') as order_id,
        nvl(ext_data,'') as ext_data,
        nvl(start_time,'') as record_start_time,
        nvl(end_time,'') as record_end_time,
        nvl(status,-99999998) as status,
        case when status = 1 then '空闲'
             when status = 2 then '测试中'
             when status = 3 then '演示中'
             when status = 4 then '运营中'
             when status = 5 then '交付中'
             when status = 6 then '待回收'
             when status = 7 then '正常'
             when status = 8 then '停用'
             when status = 8 then '故障'
             when status = 10 then '维修中'
             else '未知'
        end as status_name,
        nvl(cause,-99999998) as cause_type,
        case when cause = 1 then '达闼原因'
             when cause = 2 then '客户原因'
             else '未知'
        end as cause_type_name,
        nvl(cast(staff_id as string),'') as staff_id,
        nvl(staff_name,'') as staff_name,
        case when length(update_time) = 10 then nvl(concat(from_unixtime( cast(update_time as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
             when length(update_time) = 19 then nvl(concat(from_utc_timestamp(update_time,'PRC'),'.000'),'')
             when length(update_time) > 19 and length(update_time) < 23 then if(LENGTH(from_utc_timestamp(update_time,'PRC'))=19,nvl(concat(from_utc_timestamp(update_time,'PRC'),'.000'),''),nvl(rpad(from_utc_timestamp(update_time,'PRC'),23,'0'),''))
             when length(update_time) >= 23 then nvl(from_utc_timestamp(SUBSTRING(REGEXP_REPLACE(update_time,'T',' '),0,23),'PRC'),'')
            -- when length(update_time) = 19 then nvl(concat(update_time,'.000'),'')
            -- when length(update_time) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(update_time,'T',' '),0,23),'')
             else nvl(concat(from_unixtime( cast(update_time/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(update_time,11,3)),'')
        end as event_time,
        nvl(k8s_env_name,'') as k8s_env_name,
        case when length(create_time) = 10 then nvl(concat(from_unixtime( cast(create_time as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
             when length(create_time) = 19 then nvl(concat(from_utc_timestamp(create_time,'PRC'),'.000'),'')
             when length(create_time) > 19 and length(create_time) < 23 then if(LENGTH(from_utc_timestamp(create_time,'PRC'))=19,nvl(concat(from_utc_timestamp(create_time,'PRC'),'.000'),''),nvl(rpad(from_utc_timestamp(create_time,'PRC'),23,'0'),''))
             when length(create_time) >= 23 then nvl(from_utc_timestamp(SUBSTRING(REGEXP_REPLACE(create_time,'T',' '),0,23),'PRC'),'')
             --  when length(create_time) = 19 then nvl(concat(create_time,'.000'),'')
             --  when length(create_time) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(create_time,'T',' '),0,23),'')
             else nvl(concat(from_unixtime( cast(create_time/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(create_time,11,3)),'')
        end as create_time,
        case when length(update_time) = 10 then nvl(concat(from_unixtime( cast(update_time as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
             when length(update_time) = 19 then nvl(concat(from_utc_timestamp(update_time,'PRC'),'.000'),'')
             when length(update_time) > 19 and length(update_time) < 23 then if(LENGTH(from_utc_timestamp(update_time,'PRC'))=19,nvl(concat(from_utc_timestamp(update_time,'PRC'),'.000'),''),nvl(rpad(from_utc_timestamp(update_time,'PRC'),23,'0'),''))
             when length(update_time) >= 23 then nvl(from_utc_timestamp(SUBSTRING(REGEXP_REPLACE(update_time,'T',' '),0,23),'PRC'),'')
            -- when length(update_time) = 19 then nvl(concat(update_time,'.000'),'')
            -- when length(update_time) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(update_time,'T',' '),0,23),'')
             else nvl(concat(from_unixtime( cast(update_time/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(update_time,11,3)),'')
        end as update_time
    from cdmods.ods_crio_db_c0001_t_asset_record_i_d
)
insert overwrite table cdmdwd.dwd_crio_pmd_asset_record_a_d
    select
        asset_record_id,
        asset_code,
        robot_id,
        action_type,
        action_type_name,
        tenant_id,
        customer_id,
        order_id,
        ext_data,
        record_start_time,
        record_end_time,
        status,
        status_name,
        cause_type,
        cause_type_name,
        staff_id,
        staff_name,
        event_time,
        k8s_env_name,
        create_time,
        update_time
    from crio_asset_record;