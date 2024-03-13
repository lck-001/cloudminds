set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;
set mapreduce.job.queuename=root.users.liuhao;

-- 获取当前表的最大快照日期，用来判断变化的数据，如果未获取到，需先执行手动脚本初始化数据
with roc_t_dict as (
    select
        t1.robot_type_id,
        t1.robot_type_name,
        t2.robot_type_inner_name
    from (
         select
             label as robot_type_name,
             `value` as robot_type_id
         from cdmods.ods_roc_db_c0002_t_dict_s_d
         where dt = '${e_dt_var}' and TYPE = 'robot-type'
         group by label, `value`
    ) t1
    full join (
        select
            label as robot_type_inner_name,
            `value` as robot_type_id
        from cdmods.ods_roc_db_c0002_t_dict_s_d
        where dt = '${e_dt_var}' and TYPE = 'robot-type-hari'
        group by label, `value`
    ) t2 on t1.robot_type_id = t2.robot_type_id
),
-- 机器人制造商表 type=2
crio_t_dict as (
    select
        trim(code) as robot_manufacturer_id,
        trim(name) as robot_manufacturer_name
    from cdmods.ods_crio_db_c0001_t_dict_s_d where dt = '${e_dt_var}' and `type` = 2
),
crio_t_device as (
    select
        x1.robot_id,
        x1.robot_name,
        x1.robot_type_id,
        nvl(x2.robot_type_inner_name,'') as robot_type_inner_name,
        x1.robot_type_name,
        x1.asset_code,
        x1.asset_type,
        x1.product_type_code,
        x1.product_type_code_name,
        x1.product_id,
        x1.product_id_name,
        x1.assets_status,
        x1.assets_status_name,
        x1.status,
        x1.status_name,
        nvl(x1.robot_manufacturer_id,'') as robot_manufacturer_id,
        case when nvl(x3.robot_manufacturer_name,'') != '' then x3.robot_manufacturer_name
             else nvl(x1.robot_manufacturer_name,'')
            end as robot_manufacturer_name,
        x1.model,
        x1.os,
        x1.version_code,
        x1.software_version,
        x1.hardware_version,
        x1.head_url,
        x1.regist_time,
        x1.bind_status,
        x1.bind_status_name,
        x1.is_hi_service,
        x1.is_privacy_enhance,
        x1.remind_policy_id,
        x1.sku,
        x1.quality_date,
        x1.customer_quality_date,
        x1.out_type_state,
        x1.out_type_state_name,
        x1.product_date,
        x1.in_stock_date,
        x1.out_stock_date,
        x1.in_stock_staff_id,
        x1.in_stock_staff_name,
        x1.out_stock_staff_id,
        x1.out_stock_staff_name,
        x1.note,
        x1.description,
        x1.create_time,
        x1.update_time,
        x1.k8s_env_name
    from (
         select
             nvl(t1.device_code,'') as robot_id,
             nvl(t1.device_name,'') as robot_name,
             nvl(t2.robot_type_id,'') as robot_type_id,
             nvl(t2.robot_type_name,'') as robot_type_name,
             nvl(t1.asset_code,'') as asset_code,
             nvl(t1.asset_type,'') as asset_type,
             nvl(t1.product_type_code,'') as product_type_code,
             nvl(t1.product_type_code_name,'') as product_type_code_name,
             nvl(t1.product_id,'') as product_id,
             nvl(t1.product_id_name,'') as product_id_name,
             nvl(t1.assets_status,-99999998) as assets_status,
             case when t1.assets_status = 0 then '在册'
                  when t1.assets_status = 1 then '在库'
                  when t1.assets_status = 2 then '出库'
                  else '未知'
                 end as assets_status_name,
             case when t1.status = 1 then 0
                  when t1.status = 9 then -1
                  else -99999998
                 end as status,
             case when t1.status = 1 then '正常'
                  when t1.status = 9 then '删除'
                  else '未知'
                 end as status_name,
             nvl(supplier_code,'') as robot_manufacturer_id,
             nvl(supplier_code_name,'') as robot_manufacturer_name,
             nvl(t1.device_model,'') as model,
             '' as os,
             '' as version_code,
             nvl(t1.software_version,'') as software_version,
             nvl(t1.hardware_version,'') as hardware_version,
             '' as head_url,
             '' as regist_time,
             -99999998 as bind_status,
             '未知' as bind_status_name,
             -99999998 as is_hi_service,
             -99999998 as is_privacy_enhance,
             '' as remind_policy_id,
             nvl(t1.sku,'') as sku,
             case when length(t1.quality_date) = 10 then nvl(concat(from_unixtime( cast(t1.quality_date as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
                  when length(t1.quality_date) = 19 then nvl(concat(from_utc_timestamp(t1.quality_date,'PRC'),'.000'),'')
                  when length(t1.quality_date) >= 23 then nvl(from_utc_timestamp(SUBSTRING(REGEXP_REPLACE(t1.quality_date,'T',' '),0,23),'PRC'),'')
--                   when length(t1.quality_date) = 19 then nvl(concat(t1.quality_date,'.000'),'')
--                   when length(t1.quality_date) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(t1.quality_date,'T',' '),0,23),'')
                  else nvl(concat(from_unixtime( cast(t1.quality_date/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(t1.quality_date,11,3)),'')
             end as quality_date,
             case when length(t1.customer_quality_date) = 10 then nvl(concat(from_unixtime( cast(t1.customer_quality_date as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
                  when length(t1.customer_quality_date) = 19 then nvl(concat(from_utc_timestamp(t1.customer_quality_date,'PRC'),'.000'),'')
                  when length(t1.customer_quality_date) >= 23 then nvl(from_utc_timestamp(SUBSTRING(REGEXP_REPLACE(t1.customer_quality_date,'T',' '),0,23),'PRC'),'')
--                   when length(t1.customer_quality_date) = 19 then nvl(concat(t1.customer_quality_date,'.000'),'')
--                   when length(t1.customer_quality_date) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(t1.customer_quality_date,'T',' '),0,23),'')
                  else nvl(concat(from_unixtime( cast(t1.customer_quality_date/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(t1.customer_quality_date,11,3)),'')
             end as customer_quality_date,
             nvl(out_type_state,-99999998) as out_type_state,
             case when out_type_state = 0 then '交付出库'
                  when out_type_state = 1 then '维修出库'
                  when out_type_state = 2 then '报废出库'
                  when out_type_state = 3 then '交付给客户'
                  when out_type_state = 4 then '交付给测试'
                  else '未知'
                 end as out_type_state_name,
             case when length(t1.product_date) = 10 then nvl(concat(from_unixtime( cast(t1.product_date as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
                  when length(t1.product_date) = 19 then nvl(concat(from_utc_timestamp(t1.product_date,'PRC'),'.000'),'')
                  when length(t1.product_date) >= 23 then nvl(from_utc_timestamp(SUBSTRING(REGEXP_REPLACE(t1.product_date,'T',' '),0,23),'PRC'),'')
--                   when length(t1.product_date) = 19 then nvl(concat(t1.product_date,'.000'),'')
--                   when length(t1.product_date) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(t1.product_date,'T',' '),0,23),'')
                  else nvl(concat(from_unixtime( cast(t1.product_date/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(t1.product_date,11,3)),'')
             end as product_date,
             case when length(t1.in_stock_date) = 10 then nvl(concat(from_unixtime( cast(t1.in_stock_date as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
                  when length(t1.in_stock_date) = 19 then nvl(concat(from_utc_timestamp(t1.in_stock_date,'PRC'),'.000'),'')
                  when length(t1.in_stock_date) >= 23 then nvl(from_utc_timestamp(SUBSTRING(REGEXP_REPLACE(t1.in_stock_date,'T',' '),0,23),'PRC'),'')
--                   when length(t1.in_stock_date) = 19 then nvl(concat(t1.in_stock_date,'.000'),'')
--                   when length(t1.in_stock_date) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(t1.in_stock_date,'T',' '),0,23),'')
                  else nvl(concat(from_unixtime( cast(t1.in_stock_date/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(t1.in_stock_date,11,3)),'')
             end as in_stock_date,
             case when length(t1.out_stock_date) = 10 then nvl(concat(from_unixtime( cast(t1.out_stock_date as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
                  when length(t1.out_stock_date) = 19 then nvl(concat(from_utc_timestamp(t1.out_stock_date,'PRC'),'.000'),'')
                  when length(t1.out_stock_date) >= 23 then nvl(from_utc_timestamp(SUBSTRING(REGEXP_REPLACE(t1.out_stock_date,'T',' '),0,23),'PRC'),'')
--                   when length(t1.out_stock_date) = 19 then nvl(concat(t1.out_stock_date,'.000'),'')
--                   when length(t1.out_stock_date) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(t1.out_stock_date,'T',' '),0,23),'')
                  else nvl(concat(from_unixtime( cast(t1.out_stock_date/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(t1.out_stock_date,11,3)),'')
             end as out_stock_date,
             nvl(t1.in_stock_staff_id,'') as in_stock_staff_id,
             nvl(t1.in_stock_staff_name,'') as in_stock_staff_name,
             nvl(t1.out_stock_staff_id,'') as out_stock_staff_id,
             nvl(t1.out_stock_staff_name,'') as out_stock_staff_name,
             nvl(t1.note,'') as note,
             '' as description,
             case when length(t1.create_time) = 10 then nvl(concat(from_unixtime( cast(t1.create_time as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
                  when length(t1.create_time) = 19 then nvl(concat(from_utc_timestamp(t1.create_time,'PRC'),'.000'),'')
                  when length(t1.create_time) >= 23 then nvl(from_utc_timestamp(SUBSTRING(REGEXP_REPLACE(t1.create_time,'T',' '),0,23),'PRC'),'')
--                   when length(t1.create_time) = 19 then nvl(concat(t1.create_time,'.000'),'')
--                   when length(t1.create_time) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(t1.create_time,'T',' '),0,23),'')
                  else nvl(concat(from_unixtime( cast(t1.create_time/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(t1.create_time,11,3)),'')
             end as create_time,
             case when length(t1.update_time) = 10 then nvl(concat(from_unixtime( cast(t1.update_time as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
                  when length(t1.update_time) = 19 then nvl(concat(from_utc_timestamp(t1.update_time,'PRC'),'.000'),'')
                  when length(t1.update_time) >= 23 then nvl(from_utc_timestamp(SUBSTRING(REGEXP_REPLACE(t1.update_time,'T',' '),0,23),'PRC'),'')
--                   when length(t1.update_time) = 19 then nvl(concat(t1.update_time,'.000'),'')
--                   when length(t1.update_time) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(t1.update_time,'T',' '),0,23),'')
                  else nvl(concat(from_unixtime( cast(t1.update_time/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(t1.update_time,11,3)),'')
             end as update_time,
             'bj-prod-232' as k8s_env_name
         from cdmods.ods_crio_db_c0001_t_device_i_d t1
         left join (
             select
                 robot_type_id,
                 robot_type_name
             from roc_t_dict
         ) t2 on trim(lower(regexp_replace(t1.product_type_code,'\\s',''))) = trim(lower(regexp_replace(t2.robot_type_name,'\\s','')))
         where dt >= '${s_dt_var}' and dt <= '${e_dt_var}'
         ) x1 left join (
        select
            robot_type_id,
            robot_type_inner_name
        from roc_t_dict
    ) x2 on x1.robot_type_id = x2.robot_type_id
   left join crio_t_dict x3 on nvl(trim(x1.robot_manufacturer_id),'') = x3.robot_manufacturer_id
),
-- 取出roc中的机器人设备
roc_t_robot as (
    select
        t1.robot_id,
        t1.robot_name,
        t1.robot_type_id,
        nvl(t2.robot_type_inner_name,'') as robot_type_inner_name,
        nvl(t2.robot_type_name,'') as robot_type_name,
        t1.asset_code,
        t1.asset_type,
        t1.product_type_code,
        t1.product_type_code_name,
        t1.product_id,
        t1.product_id_name,
        t1.assets_status,
        t1.assets_status_name,
        t1.status,
        t1.status_name,
        nvl(t1.robot_manufacturer_id,'') as robot_manufacturer_id,
        case when nvl(t3.robot_manufacturer_name,'') != '' then t3.robot_manufacturer_name
             else nvl(t1.robot_manufacturer_name,'')
        end as robot_manufacturer_name,
        t1.model,
        t1.os,
        t1.version_code,
        t1.software_version,
        t1.hardware_version,
        t1.head_url,
        t1.regist_time,
        t1.bind_status,
        t1.bind_status_name,
        t1.is_hi_service,
        t1.is_privacy_enhance,
        t1.remind_policy_id,
        t1.sku,
        t1.quality_date,
        t1.customer_quality_date,
        t1.out_type_state,
        t1.out_type_state_name,
        t1.product_date,
        t1.in_stock_date,
        t1.out_stock_date,
        t1.in_stock_staff_id,
        t1.in_stock_staff_name,
        t1.out_stock_staff_id,
        t1.out_stock_staff_name,
        t1.note,
        t1.description,
        t1.create_time,
        t1.update_time,
        t1.k8s_env_name
    from (
         select
             nvl(robot_code,'') as robot_id,
             nvl(robot_name,'') as robot_name,
             cast(nvl(robot_type,'') as string) as robot_type_id,
             '' as asset_code,
             '' as asset_type,
             '' as product_type_code,
             '' as product_type_code_name,
             '' as product_id,
             '' as product_id_name,
             -99999998 as assets_status,
             '未知' as assets_status_name,
             nvl(status,-99999998) as status,
             case when status = 0 then '正常'
                  when status = 1 then '停用'
                  when status = -1 then '删除'
                  else '未知'
             end as status_name,
             manufacturer as robot_manufacturer_id,
             '' as robot_manufacturer_name,
             nvl(model,'') as model,
             nvl(os,'') as os,
             nvl(version_code,'') as version_code,
             '' as software_version,
             '' as hardware_version,
             nvl(head_url,'') as head_url,
             nvl(regist_time,'') as regist_time,
             nvl(bind_status,-99999998) as bind_status,
             case when bind_status = -1 then '删除'
                  when bind_status = 0 then '注册未激活'
                  when bind_status = 1 then '激活可用'
                  when bind_status = 2 then '通知VPN注册'
                  else '未知'
             end as bind_status_name,
             nvl(hi_service,-99999998) as is_hi_service,
             nvl(privacy_enhance,-99999998) as is_privacy_enhance,
             nvl(remind_policy_id,'') as remind_policy_id,
             nvl(sku,'') as sku,
             '' as quality_date,
             '' as customer_quality_date,
             -99999998 as out_type_state,
             '未知' as out_type_state_name,
             '' as product_date,
             '' as in_stock_date,
             '' as out_stock_date,
             '' as in_stock_staff_id,
             '' as in_stock_staff_name,
             '' as out_stock_staff_id,
             '' as out_stock_staff_name,
             '' as note,
             regexp_replace(nvl(trim(description),''), '\r|\n|\t', '') as description,
             case when length(regist_time) = 10 then nvl(concat(from_unixtime( cast(regist_time as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
                  when length(regist_time) = 19 then nvl(concat(regist_time,'.000'),'')
                  when length(regist_time) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(regist_time,'T',' '),0,23),'')
                  else nvl(concat(from_unixtime( cast(regist_time/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(regist_time,11,3)),'')
             end as create_time,
             case when length(update_time) = 10 then nvl(concat(from_unixtime( cast(update_time as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
                  when length(update_time) = 19 then nvl(concat(update_time,'.000'),'')
                  when length(update_time) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(update_time,'T',' '),0,23),'')
                  else nvl(concat(from_unixtime( cast(update_time/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(update_time,11,3)),'')
             end as update_time,
             k8s_env_name
         from cdmods.ods_roc_db_c0002_t_robot_s_d where dt <= '${e_dt_var}'
         union all
         select
             nvl(robot_code,'') as robot_id,
             nvl(robot_name,'') as robot_name,
             cast(nvl(robot_type,'') as string) as robot_type_id,
             '' as asset_code,
             '' as asset_type,
             '' as product_type_code,
             '' as product_type_code_name,
             '' as product_id,
             '' as product_id_name,
             -99999998 as assets_status,
             '未知' as assets_status_name,
             nvl(status,-99999998) as status,
             case when status = 0 then '正常'
                  when status = 1 then '停用'
                  when status = -1 then '删除'
                  else '未知'
                 end as status_name,
             manufacturer as robot_manufacturer_id,
             '' as robot_manufacturer_name,
             nvl(model,'') as model,
             nvl(os,'') as os,
             nvl(version_code,'') as version_code,
             '' as software_version,
             '' as hardware_version,
             nvl(head_url,'') as head_url,
             nvl(regist_time,'') as regist_time,
             nvl(bind_status,-99999998) as bind_status,
             case when bind_status = -1 then '删除'
                  when bind_status = 0 then '注册未激活'
                  when bind_status = 1 then '激活可用'
                  when bind_status = 2 then '通知VPN注册'
                  else '未知'
                 end as bind_status_name,
             nvl(hi_service,-99999998) as is_hi_service,
             nvl(privacy_enhance,-99999998) as is_privacy_enhance,
             nvl(remind_policy_id,'') as remind_policy_id,
             nvl(sku,'') as sku,
             '' as quality_date,
             '' as customer_quality_date,
             -99999998 as out_type_state,
             '未知' as out_type_state_name,
             '' as product_date,
             '' as in_stock_date,
             '' as out_stock_date,
             '' as in_stock_staff_id,
             '' as in_stock_staff_name,
             '' as out_stock_staff_id,
             '' as out_stock_staff_name,
             '' as note,
             regexp_replace(nvl(trim(description),''), '\r|\n|\t', '') as description,
             case when length(regist_time) = 10 then nvl(concat(from_unixtime( cast(regist_time as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
                  when length(regist_time) = 19 then nvl(concat(regist_time,'.000'),'')
                  when length(regist_time) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(regist_time,'T',' '),0,23),'')
                  else nvl(concat(from_unixtime( cast(regist_time/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(regist_time,11,3)),'')
                 end as create_time,
             case when length(update_time) = 10 then nvl(concat(from_unixtime( cast(update_time as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
                  when length(update_time) = 19 then nvl(concat(update_time,'.000'),'')
                  when length(update_time) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(update_time,'T',' '),0,23),'')
                  else nvl(concat(from_unixtime( cast(update_time/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(update_time,11,3)),'')
                 end as update_time,
             k8s_env_name
         from cdmods.ods_roc_db_c0008_t_robot_s_d where dt <= '${e_dt_var}'
         ) t1 left join (
        select
            robot_type_id,
            robot_type_name,
            robot_type_inner_name
        from roc_t_dict
    ) t2 on t1.robot_type_id = t2.robot_type_id
    left join crio_t_dict t3 on nvl(trim(t1.robot_manufacturer_id),'') = t3.robot_manufacturer_id
    where substring(t1.update_time,0,10) <= '${e_dt_var}'
),
-- join 不成功 保留作为新数据
-- join 成功 先补充维度信息，然后和dt = '${e_dt_var}' and end_time = '9999-12-31 23:59:59.999' 进行排序 lead
un_join_data as (
    select
        case when t1.robot_id is not null and t2.robot_id is null then nvl(t1.robot_id,'')
             else nvl(t2.robot_id,'')
            end as robot_id,
        case when t1.robot_id is not null and t2.robot_id is null then nvl(t1.robot_name,'')
             else nvl(t2.robot_name,'')
            end as robot_name,
        case when t1.robot_id is not null and t2.robot_id is null then nvl(t1.robot_type_id,'')
             else nvl(t2.robot_type_id,'')
            end as robot_type_id,
        case when t1.robot_id is not null and t2.robot_id is null then nvl(t1.robot_type_inner_name,'')
             else nvl(t2.robot_type_inner_name,'')
            end as robot_type_inner_name,
        case when t1.robot_id is not null and t2.robot_id is null then nvl(t1.robot_type_name,'')
             else nvl(t2.robot_type_name,'')
            end as robot_type_name,
        case when t1.robot_id is not null and t2.robot_id is null then nvl(t1.asset_code,'')
             else nvl(t2.asset_code,'')
            end as asset_code,
        case when t1.robot_id is not null and t2.robot_id is null then nvl(t1.asset_type,'')
             else nvl(t2.asset_type,'')
            end as asset_type,
        case when t1.robot_id is not null and t2.robot_id is null then nvl(t1.product_type_code,'')
             else nvl(t2.product_type_code,'')
            end as product_type_code,
        case when t1.robot_id is not null and t2.robot_id is null then nvl(t1.product_type_code_name,'')
             else nvl(t2.product_type_code_name,'')
            end as product_type_code_name,
        case when t1.robot_id is not null and t2.robot_id is null then nvl(t1.product_id,'')
             else nvl(t2.product_id,'')
            end as product_id,
        case when t1.robot_id is not null and t2.robot_id is null then nvl(t1.product_id_name,'')
             else nvl(t2.product_id_name,'')
            end as product_id_name,
        case when t1.robot_id is not null and t2.robot_id is null then nvl(t1.assets_status,-99999998)
             else nvl(t2.assets_status,-99999998)
            end as assets_status,
        case when t1.robot_id is not null and t2.robot_id is null then nvl(t1.assets_status_name,'未知')
             else nvl(t2.assets_status_name,'未知')
            end as assets_status_name,
        case when t1.robot_id is not null and t2.robot_id is null then nvl(t1.status,-99999998)
             else nvl(t2.status,-99999998)
            end as status,
        case when t1.robot_id is not null and t2.robot_id is null then nvl(t1.status_name,'未知')
             else nvl(t2.status_name,'未知')
            end as status_name,
        case when t1.robot_id is not null and t2.robot_id is null then nvl(t1.robot_manufacturer_id,'')
             else nvl(t2.robot_manufacturer_id,'')
            end as robot_manufacturer_id,
        case when t1.robot_id is not null and t2.robot_id is null then nvl(t1.robot_manufacturer_name,'')
             else nvl(t2.robot_manufacturer_name,'')
            end as robot_manufacturer_name,
        case when t1.robot_id is not null and t2.robot_id is null then nvl(t1.model,'')
             else nvl(t2.model,'')
            end as model,
        case when t1.robot_id is not null and t2.robot_id is null then nvl(t1.os,'')
             else nvl(t2.os,'')
            end as os,
        case when t1.robot_id is not null and t2.robot_id is null then nvl(t1.version_code,'')
             else nvl(t2.version_code,'')
            end as version_code,
        case when t1.robot_id is not null and t2.robot_id is null then nvl(t1.software_version,'')
             else nvl(t2.software_version,'')
            end as software_version,
        case when t1.robot_id is not null and t2.robot_id is null then nvl(t1.hardware_version,'')
             else nvl(t2.hardware_version,'')
            end as hardware_version,
        case when t1.robot_id is not null and t2.robot_id is null then nvl(t1.head_url,'')
             else nvl(t2.head_url,'')
            end as head_url,
        case when t1.robot_id is not null and t2.robot_id is null then nvl(t1.regist_time,'')
             else nvl(t2.regist_time,'')
            end as regist_time,
        case when t1.robot_id is not null and t2.robot_id is null then nvl(t1.bind_status,-99999998)
             else nvl(t2.bind_status,-99999998)
            end as bind_status,
        case when t1.robot_id is not null and t2.robot_id is null then nvl(t1.bind_status_name,'未知')
             else nvl(t2.bind_status_name,'未知')
            end as bind_status_name,
        case when t1.robot_id is not null and t2.robot_id is null then nvl(t1.is_hi_service,-99999998)
             else nvl(t2.is_hi_service,-99999998)
            end as is_hi_service,
        case when t1.robot_id is not null and t2.robot_id is null then nvl(t1.is_privacy_enhance,-99999998)
             else nvl(t2.is_privacy_enhance,-99999998)
            end as is_privacy_enhance,
        case when t1.robot_id is not null and t2.robot_id is null then nvl(t1.remind_policy_id,'')
             else nvl(t2.remind_policy_id,'')
            end as remind_policy_id,
        case when t1.robot_id is not null and t2.robot_id is null then nvl(t1.sku,'')
             else nvl(t2.sku,'')
            end as sku,
        case when t1.robot_id is not null and t2.robot_id is null then nvl(t1.quality_date,'')
             else nvl(t2.quality_date,'')
            end as quality_date,
        case when t1.robot_id is not null and t2.robot_id is null then nvl(t1.customer_quality_date,'')
             else nvl(t2.customer_quality_date,'')
            end as customer_quality_date,
        case when t1.robot_id is not null and t2.robot_id is null then nvl(t1.out_type_state,-99999998)
             else nvl(t2.out_type_state,-99999998)
            end as out_type_state,
        case when t1.robot_id is not null and t2.robot_id is null then nvl(t1.out_type_state_name,'未知')
             else nvl(t2.out_type_state_name,'未知')
            end as out_type_state_name,
        case when t1.robot_id is not null and t2.robot_id is null then nvl(t1.product_date,'')
             else nvl(t2.product_date,'')
            end as product_date,
        case when t1.robot_id is not null and t2.robot_id is null then nvl(t1.in_stock_date,'')
             else nvl(t2.in_stock_date,'')
            end as in_stock_date,
        case when t1.robot_id is not null and t2.robot_id is null then nvl(t1.out_stock_date,'')
             else nvl(t2.out_stock_date,'')
            end as out_stock_date,
        case when t1.robot_id is not null and t2.robot_id is null then nvl(t1.in_stock_staff_id,'')
             else nvl(t2.in_stock_staff_id,'')
            end as in_stock_staff_id,
        case when t1.robot_id is not null and t2.robot_id is null then nvl(t1.in_stock_staff_name,'')
             else nvl(t2.in_stock_staff_name,'')
            end as in_stock_staff_name,
        case when t1.robot_id is not null and t2.robot_id is null then nvl(t1.out_stock_staff_id,'')
             else nvl(t2.out_stock_staff_id,'')
            end as out_stock_staff_id,
        case when t1.robot_id is not null and t2.robot_id is null then nvl(t1.out_stock_staff_name,'')
             else nvl(t2.out_stock_staff_name,'')
            end as out_stock_staff_name,
        case when t1.robot_id is not null and t2.robot_id is null then nvl(t1.note,'')
             else nvl(t2.note,'')
            end as note,
        case when t1.robot_id is not null and t2.robot_id is null then nvl(t1.description,'')
             else nvl(t2.description,'')
            end as description,
        case when t1.robot_id is not null and t2.robot_id is null then nvl(t1.update_time,'')
             else nvl(t2.update_time,'')
            end as start_time,
        '9999-12-31 23:59:59.999' as end_time,
        case when t1.robot_id is not null and t2.robot_id is null then nvl(t1.create_time,'')
             else nvl(t2.create_time,'')
            end as create_time,
        case when t1.robot_id is not null and t2.robot_id is null then nvl(t1.update_time,'')
             else nvl(t2.update_time,'')
            end as update_time,
        case when t1.robot_id is not null and t2.robot_id is null then nvl(t1.k8s_env_name,'')
             else nvl(t2.k8s_env_name,'')
            end as k8s_env_name
    from (
         select
             robot_id,
             robot_name,
             robot_type_id,
             robot_type_inner_name,
             robot_type_name,
             asset_code,
             asset_type,
             product_type_code,
             product_type_code_name,
             product_id,
             product_id_name,
             assets_status,
             assets_status_name,
             status,
             status_name,
             robot_manufacturer_id,
             robot_manufacturer_name,
             model,
             os,
             version_code,
             software_version,
             hardware_version,
             head_url,
             regist_time,
             bind_status,
             bind_status_name,
             is_hi_service,
             is_privacy_enhance,
             remind_policy_id,
             sku,
             quality_date,
             customer_quality_date,
             out_type_state,
             out_type_state_name,
             product_date,
             in_stock_date,
             out_stock_date,
             in_stock_staff_id,
             in_stock_staff_name,
             out_stock_staff_id,
             out_stock_staff_name,
             note,
             description,
             k8s_env_name,
             create_time,
             update_time
         from crio_t_device
         union
         select
             robot_id,
             robot_name,
             robot_type_id,
             robot_type_inner_name,
             robot_type_name,
             asset_code,
             asset_type,
             product_type_code,
             product_type_code_name,
             product_id,
             product_id_name,
             assets_status,
             assets_status_name,
             status,
             status_name,
             robot_manufacturer_id,
             robot_manufacturer_name,
             model,
             os,
             version_code,
             software_version,
             hardware_version,
             head_url,
             regist_time,
             bind_status,
             bind_status_name,
             is_hi_service,
             is_privacy_enhance,
             remind_policy_id,
             sku,
             quality_date,
             customer_quality_date,
             out_type_state,
             out_type_state_name,
             product_date,
             in_stock_date,
             out_stock_date,
             in_stock_staff_id,
             in_stock_staff_name,
             out_stock_staff_id,
             out_stock_staff_name,
             note,
             description,
             k8s_env_name,
             create_time,
             update_time
         from roc_t_robot
         ) t1
         left join (
            select
                robot_id,
                robot_name,
                robot_type_id,
                robot_type_inner_name,
                robot_type_name,
                asset_code,
                asset_type,
                product_type_code,
                product_type_code_name,
                product_id,
                product_id_name,
                assets_status,
                assets_status_name,
                status,
                status_name,
                robot_manufacturer_id,
                robot_manufacturer_name,
                model,
                os,
                version_code,
                software_version,
                hardware_version,
                head_url,
                regist_time,
                bind_status,
                bind_status_name,
                is_hi_service,
                is_privacy_enhance,
                remind_policy_id,
                sku,
                quality_date,
                customer_quality_date,
                out_type_state,
                out_type_state_name,
                product_date,
                in_stock_date,
                out_stock_date,
                in_stock_staff_id,
                in_stock_staff_name,
                out_stock_staff_id,
                out_stock_staff_name,
                note,
                description,
                start_time,
                end_time,
                k8s_env_name,
                create_time,
                update_time
            from cdmdim.dim_pmd_robot_sh_d
            where dt = date_sub('${e_dt_var}',1) and end_time = '9999-12-31 23:59:59.999'
    ) t2 on t1.robot_id = t2.robot_id
    where t2.robot_id is null
),
join_data as (
    select
        robot_id,
        robot_name,
        robot_type_id,
        robot_type_inner_name,
        robot_type_name,
        asset_code,
        asset_type,
        product_type_code,
        product_type_code_name,
        product_id,
        product_id_name,
        assets_status,
        assets_status_name,
        status,
        status_name,
        robot_manufacturer_id,
        robot_manufacturer_name,
        model,
        os,
        version_code,
        software_version,
        hardware_version,
        head_url,
        regist_time,
        bind_status,
        bind_status_name,
        is_hi_service,
        is_privacy_enhance,
        remind_policy_id,
        sku,
        quality_date,
        customer_quality_date,
        out_type_state,
        out_type_state_name,
        product_date,
        in_stock_date,
        out_stock_date,
        in_stock_staff_id,
        in_stock_staff_name,
        out_stock_staff_id,
        out_stock_staff_name,
        note,
        description,
        start_time,
        end_time,
        k8s_env_name,
        create_time,
        update_time
    from cdmdim.dim_pmd_robot_sh_d
    where dt = date_sub('${e_dt_var}',1)
    union
    select
        coalesce(if(nvl(t2.robot_id,'') = '',null,t2.robot_id),if(nvl(t1.robot_id,'') = '',null,t1.robot_id),'') as robot_id,
        coalesce(if(nvl(t2.robot_name,'') = '',null,t2.robot_name),if(nvl(t1.robot_name,'') = '',null,t1.robot_name),'') as robot_name,
        coalesce(if(nvl(t2.robot_type_id,'') = '',null,t2.robot_type_id),if(nvl(t1.robot_type_id,'') = '',null,t1.robot_type_id),'') as robot_type_id,
        coalesce(if(nvl(t2.robot_type_inner_name,'') = '',null,t2.robot_type_inner_name),if(nvl(t1.robot_type_inner_name,'') = '',null,t1.robot_type_inner_name),'') as robot_type_inner_name,
        coalesce(if(nvl(t2.robot_type_name,'') = '',null,t2.robot_type_name),if(nvl(t1.robot_type_name,'') = '',null,t1.robot_type_name),'') as robot_type_name,
        coalesce(if(nvl(t2.asset_code,'') = '',null,t2.asset_code),if(nvl(t1.asset_code,'') = '',null,t1.asset_code),'') as asset_code,
        coalesce(if(nvl(t2.asset_type,'') = '',null,t2.asset_type),if(nvl(t1.asset_type,'') = '',null,t1.asset_type),'') as asset_type,
        coalesce(if(nvl(t2.product_type_code,'') = '',null,t2.product_type_code),if(nvl(t1.product_type_code,'') = '',null,t1.product_type_code),'') as product_type_code,
        coalesce(if(nvl(t2.product_type_code_name,'') = '',null,t2.product_type_code_name),if(nvl(t1.product_type_code_name,'') = '',null,t1.product_type_code_name),'') as product_type_code_name,
        coalesce(if(nvl(t2.product_id,'') = '',null,t2.product_id),if(nvl(t1.product_id,'') = '',null,t1.product_id),'') as product_id,
        coalesce(if(nvl(t2.product_id_name,'') = '',null,t2.product_id_name),if(nvl(t1.product_id_name,'') = '',null,t1.product_id_name),'') as product_id_name,
        coalesce(if(nvl(t2.assets_status,-99999998) = -99999998,null,t2.assets_status),if(nvl(t1.assets_status,-99999998) = -99999998,null,t1.assets_status),-99999998) as assets_status,
        coalesce(if(nvl(t2.assets_status_name,'未知') = '未知',null,t2.assets_status_name),if(nvl(t1.assets_status_name,'未知') = '未知',null,t1.assets_status_name),'未知') as assets_status_name,
        coalesce(if(nvl(t2.status,-99999998) = -99999998,null,t2.status),if(nvl(t1.status,-99999998) = -99999998,null,t1.status),-99999998) as status,
        coalesce(if(nvl(t2.status_name,'未知') = '未知',null,t2.status_name),if(nvl(t1.status_name,'未知') = '未知',null,t1.status_name),'未知') as status_name,
        coalesce(if(nvl(t2.robot_manufacturer_id,'') = '',null,t2.robot_manufacturer_id),if(nvl(t1.robot_manufacturer_id,'') = '',null,t1.robot_manufacturer_id),'') as robot_manufacturer_id,
        coalesce(if(nvl(t2.robot_manufacturer_name,'') = '',null,t2.robot_manufacturer_name),if(nvl(t1.robot_manufacturer_name,'') = '',null,t1.robot_manufacturer_name),'') as robot_manufacturer_name,
        coalesce(if(nvl(t2.model,'') = '',null,t2.model),if(nvl(t1.model,'') = '',null,t1.model),'') as model,
        coalesce(if(nvl(t2.os,'') = '',null,t2.os),if(nvl(t1.os,'') = '',null,t1.os),'') as os,
        coalesce(if(nvl(t2.version_code,'') = '',null,t2.version_code),if(nvl(t1.version_code,'') = '',null,t1.version_code),'') as version_code,
        coalesce(if(nvl(t2.software_version,'') = '',null,t2.software_version),if(nvl(t1.software_version,'') = '',null,t1.software_version),'') as software_version,
        coalesce(if(nvl(t2.hardware_version,'') = '',null,t2.hardware_version),if(nvl(t1.hardware_version,'') = '',null,t1.hardware_version),'') as hardware_version,
        coalesce(if(nvl(t2.head_url,'') = '',null,t2.head_url),if(nvl(t1.head_url,'') = '',null,t1.head_url),'') as head_url,
        coalesce(if(nvl(t2.regist_time,'') = '',null,t2.regist_time),if(nvl(t1.regist_time,'') = '',null,t1.regist_time),'') as regist_time,
        coalesce(if(nvl(t2.bind_status,-99999998) = -99999998,null,t2.bind_status),if(nvl(t1.bind_status,-99999998) = -99999998,null,t1.bind_status),-99999998) as bind_status,
        coalesce(if(nvl(t2.bind_status_name,'未知') = '未知',null,t2.bind_status_name),if(nvl(t1.bind_status_name,'未知') = '未知',null,t1.bind_status_name),'未知') as bind_status_name,
        coalesce(if(nvl(t2.is_hi_service,-99999998) = -99999998,null,t2.is_hi_service),if(nvl(t1.is_hi_service,-99999998) = -99999998,null,t1.is_hi_service),-99999998) as is_hi_service,
        coalesce(if(nvl(t2.is_privacy_enhance,-99999998) = -99999998,null,t2.is_privacy_enhance),if(nvl(t1.is_privacy_enhance,-99999998) = -99999998,null,t1.is_privacy_enhance),-99999998) as is_privacy_enhance,
        coalesce(if(nvl(t2.remind_policy_id,'') = '',null,t2.remind_policy_id),if(nvl(t1.remind_policy_id,'') = '',null,t1.remind_policy_id),'') as remind_policy_id,
        coalesce(if(nvl(t2.sku,'') = '',null,t2.sku),if(nvl(t1.sku,'') = '',null,t1.sku),'') as sku,
        coalesce(if(nvl(t2.quality_date,'') = '',null,t2.quality_date),if(nvl(t1.quality_date,'') = '',null,t1.quality_date),'') as quality_date,
        coalesce(if(nvl(t2.customer_quality_date,'') = '',null,t2.customer_quality_date),if(nvl(t1.customer_quality_date,'') = '',null,t1.customer_quality_date),'') as customer_quality_date,
        coalesce(if(nvl(t2.out_type_state,-99999998) = -99999998,null,t2.out_type_state),if(nvl(t1.out_type_state,-99999998) = -99999998,null,t1.out_type_state),-99999998) as out_type_state,
        coalesce(if(nvl(t2.out_type_state_name,'未知') = '未知',null,t2.out_type_state_name),if(nvl(t1.out_type_state_name,'未知') = '未知',null,t1.out_type_state_name),'未知') as out_type_state_name,
        coalesce(if(nvl(t2.product_date,'') = '',null,t2.product_date),if(nvl(t1.product_date,'') = '',null,t1.product_date),'') as product_date,
        coalesce(if(nvl(t2.in_stock_date,'') = '',null,t2.in_stock_date),if(nvl(t1.in_stock_date,'') = '',null,t1.in_stock_date),'') as in_stock_date,
        coalesce(if(nvl(t2.out_stock_date,'') = '',null,t2.out_stock_date),if(nvl(t1.out_stock_date,'') = '',null,t1.out_stock_date),'') as out_stock_date,
        coalesce(if(nvl(t2.in_stock_staff_id,'') = '',null,t2.in_stock_staff_id),if(nvl(t1.in_stock_staff_id,'') = '',null,t1.in_stock_staff_id),'') as in_stock_staff_id,
        coalesce(if(nvl(t2.in_stock_staff_name,'') = '',null,t2.in_stock_staff_name),if(nvl(t1.in_stock_staff_name,'') = '',null,t1.in_stock_staff_name),'') as in_stock_staff_name,
        coalesce(if(nvl(t2.out_stock_staff_id,'') = '',null,t2.out_stock_staff_id),if(nvl(t1.out_stock_staff_id,'') = '',null,t1.out_stock_staff_id),'') as out_stock_staff_id,
        coalesce(if(nvl(t2.out_stock_staff_name,'') = '',null,t2.out_stock_staff_name),if(nvl(t1.out_stock_staff_name,'') = '',null,t1.out_stock_staff_name),'') as out_stock_staff_name,
        coalesce(if(nvl(t2.note,'') = '',null,t2.note),if(nvl(t1.note,'') = '',null,t1.note),'') as note,
        coalesce(if(nvl(t2.description,'') = '',null,t2.description),if(nvl(t1.description,'') = '',null,t1.description),'') as description,
        coalesce(if(nvl(t2.update_time,'') = '',null,t2.update_time),if(nvl(t1.update_time,'') = '',null,t1.update_time),'') as start_time,
        '9999-12-31 23:59:59.999' as end_time,
        coalesce(if(nvl(t2.k8s_env_name,'') = '',null,t2.k8s_env_name),if(nvl(t1.k8s_env_name,'') = '',null,t1.k8s_env_name),'') as k8s_env_name,
        coalesce(if(nvl(t2.create_time,'') = '',null,t2.create_time),if(nvl(t1.create_time,'') = '',null,t1.create_time),'') as create_time,
        coalesce(if(nvl(t2.update_time,'') = '',null,t2.update_time),if(nvl(t1.update_time,'') = '',null,t1.update_time),'') as update_time
    from (
         select
             robot_id,
             robot_name,
             robot_type_id,
             robot_type_inner_name,
             robot_type_name,
             asset_code,
             asset_type,
             product_type_code,
             product_type_code_name,
             product_id,
             product_id_name,
             assets_status,
             assets_status_name,
             status,
             status_name,
             robot_manufacturer_id,
             robot_manufacturer_name,
             model,
             os,
             version_code,
             software_version,
             hardware_version,
             head_url,
             regist_time,
             bind_status,
             bind_status_name,
             is_hi_service,
             is_privacy_enhance,
             remind_policy_id,
             sku,
             quality_date,
             customer_quality_date,
             out_type_state,
             out_type_state_name,
             product_date,
             in_stock_date,
             out_stock_date,
             in_stock_staff_id,
             in_stock_staff_name,
             out_stock_staff_id,
             out_stock_staff_name,
             note,
             description,
             start_time,
             end_time,
             k8s_env_name,
             create_time,
             update_time
         from cdmdim.dim_pmd_robot_sh_d
         where dt = date_sub('${e_dt_var}',1) and end_time = '9999-12-31 23:59:59.999'
         ) t1
         left join (
            select
                robot_id,
                robot_name,
                robot_type_id,
                robot_type_inner_name,
                robot_type_name,
                asset_code,
                asset_type,
                product_type_code,
                product_type_code_name,
                product_id,
                product_id_name,
                assets_status,
                assets_status_name,
                status,
                status_name,
                robot_manufacturer_id,
                robot_manufacturer_name,
                model,
                os,
                version_code,
                software_version,
                hardware_version,
                head_url,
                regist_time,
                bind_status,
                bind_status_name,
                is_hi_service,
                is_privacy_enhance,
                remind_policy_id,
                sku,
                quality_date,
                customer_quality_date,
                out_type_state,
                out_type_state_name,
                product_date,
                in_stock_date,
                out_stock_date,
                in_stock_staff_id,
                in_stock_staff_name,
                out_stock_staff_id,
                out_stock_staff_name,
                note,
                description,
                k8s_env_name,
                create_time,
                update_time
            from crio_t_device
            union
            select
                robot_id,
                robot_name,
                robot_type_id,
                robot_type_inner_name,
                robot_type_name,
                asset_code,
                asset_type,
                product_type_code,
                product_type_code_name,
                product_id,
                product_id_name,
                assets_status,
                assets_status_name,
                status,
                status_name,
                robot_manufacturer_id,
                robot_manufacturer_name,
                model,
                os,
                version_code,
                software_version,
                hardware_version,
                head_url,
                regist_time,
                bind_status,
                bind_status_name,
                is_hi_service,
                is_privacy_enhance,
                remind_policy_id,
                sku,
                quality_date,
                customer_quality_date,
                out_type_state,
                out_type_state_name,
                product_date,
                in_stock_date,
                out_stock_date,
                in_stock_staff_id,
                in_stock_staff_name,
                out_stock_staff_id,
                out_stock_staff_name,
                note,
                description,
                k8s_env_name,
                create_time,
                update_time
            from roc_t_robot
    ) t2 on t1.robot_id = t2.robot_id
    where t1.robot_id is not null and t2.robot_id is not null
),
merge_data as (
    select
        robot_id,
        robot_name,
        robot_type_id,
        robot_type_inner_name,
        robot_type_name,
        asset_code,
        asset_type,
        product_type_code,
        product_type_code_name,
        product_id,
        product_id_name,
        assets_status,
        assets_status_name,
        status,
        status_name,
        robot_manufacturer_id,
        robot_manufacturer_name,
        model,
        os,
        version_code,
        software_version,
        hardware_version,
        head_url,
        regist_time,
        bind_status,
        bind_status_name,
        is_hi_service,
        is_privacy_enhance,
        remind_policy_id,
        sku,
        quality_date,
        customer_quality_date,
        out_type_state,
        out_type_state_name,
        product_date,
        in_stock_date,
        out_stock_date,
        in_stock_staff_id,
        in_stock_staff_name,
        out_stock_staff_id,
        out_stock_staff_name,
        note,
        description,
        start_time,
        CASE WHEN tmp_end_time IS NOT NULL AND tmp_end_time != '' THEN concat(from_unixtime(cast((cast(unix_timestamp(substring(tmp_end_time,0,19),'yyyy-MM-dd HH:mm:ss') AS bigint)*1000 + cast(substring(tmp_end_time,21,23) AS bigint) - 1)/1000 as bigint),'yyyy-MM-dd HH:mm:ss'),'.',SUBSTRING((cast(unix_timestamp(substring(tmp_end_time,0,19),'yyyy-MM-dd HH:mm:ss') AS bigint)*1000 + cast(substring(tmp_end_time,21,23) AS bigint) - 1),11,13))
             ELSE '9999-12-31 23:59:59.999'
            END AS end_time,
        k8s_env_name,
        create_time,
        update_time
    from (
         select
             robot_id,
             robot_name,
             robot_type_id,
             robot_type_inner_name,
             robot_type_name,
             asset_code,
             asset_type,
             product_type_code,
             product_type_code_name,
             product_id,
             product_id_name,
             assets_status,
             assets_status_name,
             status,
             status_name,
             robot_manufacturer_id,
             robot_manufacturer_name,
             model,
             os,
             version_code,
             software_version,
             hardware_version,
             head_url,
             regist_time,
             bind_status,
             bind_status_name,
             is_hi_service,
             is_privacy_enhance,
             remind_policy_id,
             sku,
             quality_date,
             customer_quality_date,
             out_type_state,
             out_type_state_name,
             product_date,
             in_stock_date,
             out_stock_date,
             in_stock_staff_id,
             in_stock_staff_name,
             out_stock_staff_id,
             out_stock_staff_name,
             note,
             description,
             start_time,
             end_time,
             k8s_env_name,
             create_time,
             update_time,
             lead(update_time,1,NULL) over(partition BY robot_id ORDER BY update_time ASC) tmp_end_time
         from join_data
         ) t1
        union
        select
            robot_id,
            robot_name,
            robot_type_id,
            robot_type_inner_name,
            robot_type_name,
            asset_code,
            asset_type,
            product_type_code,
            product_type_code_name,
            product_id,
            product_id_name,
            assets_status,
            assets_status_name,
            status,
            status_name,
            robot_manufacturer_id,
            robot_manufacturer_name,
            model,
            os,
            version_code,
            software_version,
            hardware_version,
            head_url,
            regist_time,
            bind_status,
            bind_status_name,
            is_hi_service,
            is_privacy_enhance,
            remind_policy_id,
            sku,
            quality_date,
            customer_quality_date,
            out_type_state,
            out_type_state_name,
            product_date,
            in_stock_date,
            out_stock_date,
            in_stock_staff_id,
            in_stock_staff_name,
            out_stock_staff_id,
            out_stock_staff_name,
            note,
            description,
            start_time,
            CASE WHEN tmp_end_time IS NOT NULL AND tmp_end_time != '' THEN concat(from_unixtime(cast((cast(unix_timestamp(substring(tmp_end_time,0,19),'yyyy-MM-dd HH:mm:ss') AS bigint)*1000 + cast(substring(tmp_end_time,21,23) AS bigint) - 1)/1000 as bigint),'yyyy-MM-dd HH:mm:ss'),'.',SUBSTRING((cast(unix_timestamp(substring(tmp_end_time,0,19),'yyyy-MM-dd HH:mm:ss') AS bigint)*1000 + cast(substring(tmp_end_time,21,23) AS bigint) - 1),11,13))
                 ELSE '9999-12-31 23:59:59.999'
                END AS end_time,
            k8s_env_name,
            create_time,
            update_time
        from (
             select
                 robot_id,
                 robot_name,
                 robot_type_id,
                 robot_type_inner_name,
                 robot_type_name,
                 asset_code,
                 asset_type,
                 product_type_code,
                 product_type_code_name,
                 product_id,
                 product_id_name,
                 assets_status,
                 assets_status_name,
                 status,
                 status_name,
                 robot_manufacturer_id,
                 robot_manufacturer_name,
                 model,
                 os,
                 version_code,
                 software_version,
                 hardware_version,
                 head_url,
                 regist_time,
                 bind_status,
                 bind_status_name,
                 is_hi_service,
                 is_privacy_enhance,
                 remind_policy_id,
                 sku,
                 quality_date,
                 customer_quality_date,
                 out_type_state,
                 out_type_state_name,
                 product_date,
                 in_stock_date,
                 out_stock_date,
                 in_stock_staff_id,
                 in_stock_staff_name,
                 out_stock_staff_id,
                 out_stock_staff_name,
                 note,
                 description,
                 start_time,
                 end_time,
                 k8s_env_name,
                 create_time,
                 update_time,
                 lead(update_time,1,NULL) over(partition BY robot_id ORDER BY update_time ASC) tmp_end_time
             from un_join_data
         ) t2
),
-- 优化join
-- 补充维度，按照end_time,环境升序排序,对于空缺的属性，补充上当前robot_id,之前最近记录不为空的属性值
column_dim as (
    select
        robot_id,
        collect_set(case when robot_name is not null and robot_name != '' then concat_ws('->',robot_id,end_time,robot_name) else null end ) as robot_name_dim,
        collect_set(case when robot_type_id is not null and robot_type_id != '' then concat_ws('->',robot_id,end_time,concat_ws('###',robot_type_id,robot_type_inner_name,robot_type_name)) else null end ) as robot_type_dim,
        collect_set(case when asset_code is not null and asset_code != '' then concat_ws('->',robot_id,end_time,asset_code) else null end ) as asset_code_dim,
        collect_set(case when asset_type is not null and asset_type != '' then concat_ws('->',robot_id,end_time,asset_type) else null end ) as asset_type_dim,
        collect_set(case when product_type_code is not null and product_type_code != '' then concat_ws('->',robot_id,end_time,concat_ws('###',product_type_code,product_type_code_name)) else null end ) as product_type_dim,
        collect_set(case when (product_id is not null and product_id != '') or (product_id_name is not null and product_id_name != '') then concat_ws('->',robot_id,end_time,concat_ws('###',product_id,product_id_name)) else null end ) as product_id_dim,
        collect_set(case when assets_status is not null and trim(cast(assets_status as string)) != '' and assets_status != -99999998 then concat_ws('->',robot_id,end_time,cast(assets_status as string)) else null end ) as assets_status_dim,
        collect_set(case when status is not null and trim(cast(status as string)) != '' and status != -99999998 then concat_ws('->',robot_id,end_time,cast(status as string)) else null end ) as status_dim,
        collect_set(case when robot_manufacturer_id is not null and robot_manufacturer_id != '' then concat_ws('->',robot_id,end_time,concat_ws('###',robot_manufacturer_id,robot_manufacturer_name)) else null end ) as robot_manufacturer_dim,
        collect_set(case when model is not null and model != '' then concat_ws('->',robot_id,end_time,model) else null end ) as model_dim,
        collect_set(case when os is not null and os != '' then concat_ws('->',robot_id,end_time,os) else null end ) as os_dim,
        collect_set(case when version_code is not null and version_code != '' then concat_ws('->',robot_id,end_time,version_code) else null end ) as version_code_dim,
        collect_set(case when software_version is not null and software_version != '' then concat_ws('->',robot_id,end_time,software_version) else null end ) as software_version_dim,
        collect_set(case when hardware_version is not null and hardware_version != '' then concat_ws('->',robot_id,end_time,hardware_version) else null end ) as hardware_version_dim,
        collect_set(case when head_url is not null and head_url != '' then concat_ws('->',robot_id,end_time,head_url) else null end ) as head_url_dim,
        collect_set(case when regist_time is not null and regist_time != '' then concat_ws('->',robot_id,end_time,regist_time) else null end ) as regist_time_dim,
        collect_set(case when bind_status is not null and trim(cast(bind_status as string)) != '' and bind_status != -99999998 then concat_ws('->',robot_id,end_time,cast(bind_status as string)) else null end ) as bind_status_dim,
        collect_set(case when is_hi_service is not null and trim(cast(is_hi_service as string)) != '' and is_hi_service != -99999998 then concat_ws('->',robot_id,end_time,cast(is_hi_service as string)) else null end ) as is_hi_service_dim,
        collect_set(case when is_privacy_enhance is not null and trim(cast(is_privacy_enhance as string)) != '' and is_privacy_enhance != -99999998 then concat_ws('->',robot_id,end_time,cast(is_privacy_enhance as string)) else null end ) as is_privacy_enhance_dim,
        collect_set(case when remind_policy_id is not null and remind_policy_id != '' then concat_ws('->',robot_id,end_time,remind_policy_id) else null end ) as remind_policy_id_dim,
        collect_set(case when sku is not null and sku != '' then concat_ws('->',robot_id,end_time,sku) else null end ) as sku_dim,
        collect_set(case when quality_date is not null and quality_date != '' then concat_ws('->',robot_id,end_time,quality_date) else null end ) as quality_date_dim,
        collect_set(case when customer_quality_date is not null and customer_quality_date != '' then concat_ws('->',robot_id,end_time,customer_quality_date) else null end ) as customer_quality_date_dim,
        collect_set(case when out_type_state is not null and trim(cast(out_type_state as string)) != '' and out_type_state != -99999998 then concat_ws('->',robot_id,end_time,cast(out_type_state as string)) else null end ) as out_type_state_dim,
        collect_set(case when product_date is not null and product_date != '' then concat_ws('->',robot_id,end_time,product_date) else null end ) as product_date_dim,
        collect_set(case when in_stock_date is not null and in_stock_date != '' then concat_ws('->',robot_id,end_time,in_stock_date) else null end ) as in_stock_date_dim,
        collect_set(case when out_stock_date is not null and out_stock_date != '' then concat_ws('->',robot_id,end_time,out_stock_date) else null end ) as out_stock_date_dim,
        collect_set(case when (in_stock_staff_id is not null and in_stock_staff_id != '') or (in_stock_staff_name is not null and in_stock_staff_name != '') then concat_ws('->',robot_id,end_time,concat_ws('###',in_stock_staff_id,in_stock_staff_name)) else null end ) as in_stock_staff_dim,
        collect_set(case when (out_stock_staff_id is not null and out_stock_staff_id != '') or (out_stock_staff_name is not null and out_stock_staff_name != '') then concat_ws('->',robot_id,end_time,concat_ws('###',out_stock_staff_id,out_stock_staff_name)) else null end ) as out_stock_staff_dim,
        collect_set(case when note is not null and note != '' then concat_ws('->',robot_id,end_time,note) else null end ) as note_dim,
        collect_set(case when description is not null and description != '' then concat_ws('->',robot_id,end_time,description) else null end ) as description_dim
    from merge_data
    group by robot_id
),
 dim_repair as (
     select
         x1.robot_id,
         case when nvl(x1.robot_name_dim,'') != '' then x1.robot_name_dim
              else x1.robot_name
         end as robot_name,
         case when nvl(x1.robot_type_dim,'') != '' then split(x1.robot_type_dim,'###')[0]
              else x1.robot_type_id
         end as robot_type_id,
         case when nvl(x1.robot_type_dim,'') != '' then split(x1.robot_type_dim,'###')[1]
              else x1.robot_type_inner_name
         end as robot_type_inner_name,
         case when nvl(x1.robot_type_dim,'') != '' then split(x1.robot_type_dim,'###')[2]
              else x1.robot_type_name
         end as robot_type_name,
         case when nvl(x1.asset_code_dim,'') != '' then x1.asset_code_dim
              else x1.asset_code
         end as asset_code,
         case when nvl(x1.asset_type_dim,'') != '' then x1.asset_type_dim
              else x1.asset_type
         end as asset_type,
         case when nvl(x1.product_type_dim,'') != '' then split(x1.product_type_dim,'###')[0]
              else x1.product_type_code
         end as product_type_code,
         case when nvl(x1.product_type_dim,'') != '' then split(x1.product_type_dim,'###')[1]
              else x1.product_type_code_name
         end as product_type_code_name,
         case when nvl(x1.product_id_dim,'') != '' then split(x1.product_id_dim,'###')[0]
              else x1.product_id
         end as product_id,
         case when nvl(x1.product_id_dim,'') != '' then split(x1.product_id_dim,'###')[1]
              else x1.product_id_name
         end as product_id_name,
         case when nvl(x1.assets_status_dim,'') != '' and cast(x1.assets_status_dim as int) != -99999998 then cast(x1.assets_status_dim as int)
              else nvl(x1.assets_status,-99999998)
         end as assets_status,
         case when nvl(x1.assets_status_dim,'') != '' and cast(x1.assets_status_dim as int) = 0 then '在册'
              when nvl(x1.assets_status_dim,'') != '' and cast(x1.assets_status_dim as int) = 1 then '在库'
              when nvl(x1.assets_status_dim,'') != '' and cast(x1.assets_status_dim as int) = 2 then '出库'
              else '未知'
         end as assets_status_name,
         case when nvl(x1.status_dim,'') != '' and cast(x1.status_dim as int) != -99999998 then cast(x1.status_dim as int)
              else nvl(x1.status,-99999998)
         end as status,
         case when nvl(x1.status_dim,'') != '' and cast(x1.status_dim as int) = 0 then '正常'
              when nvl(x1.status_dim,'') != '' and cast(x1.status_dim as int) = 1 then '停用'
              when nvl(x1.status_dim,'') != '' and cast(x1.status_dim as int) = -1 then '删除'
              else '未知'
         end as status_name,
         case when nvl(x1.robot_manufacturer_dim,'') != '' then split(x1.robot_manufacturer_dim,'###')[0]
              else x1.robot_manufacturer_id
         end as robot_manufacturer_id,
         case when nvl(x1.robot_manufacturer_dim,'') != '' then split(x1.robot_manufacturer_dim,'###')[1]
              else x1.robot_manufacturer_name
         end as robot_manufacturer_name,
         case when nvl(x1.model_dim,'') != '' then x1.model_dim
              else x1.model
         end as model,
         case when nvl(x1.os_dim,'') != '' then x1.os_dim
              else x1.os
         end as os,
         case when nvl(x1.version_code_dim,'') != '' then x1.version_code_dim
              else x1.version_code
         end as version_code,
         case when nvl(x1.software_version_dim,'') != '' then x1.software_version_dim
              else x1.software_version
         end as software_version,
         case when nvl(x1.hardware_version_dim,'') != '' then x1.hardware_version_dim
              else x1.hardware_version
         end as hardware_version,
         case when nvl(x1.head_url_dim,'') != '' then x1.head_url_dim
              else x1.head_url
         end as head_url,
         case when nvl(x1.regist_time_dim,'') != '' then x1.regist_time_dim
              else x1.regist_time
         end as regist_time,
         case when nvl(x1.bind_status_dim,'') != '' and cast(x1.bind_status_dim as int) != -99999998 then cast(x1.bind_status_dim as int)
              else nvl(x1.bind_status,-99999998)
         end as bind_status,
         case when nvl(x1.bind_status_dim,'') != '' and cast(x1.bind_status_dim as int) = -1 then '删除'
              when nvl(x1.bind_status_dim,'') != '' and cast(x1.bind_status_dim as int) = 0 then '注册未激活'
              when nvl(x1.bind_status_dim,'') != '' and cast(x1.bind_status_dim as int) = 1 then '激活可用'
              when nvl(x1.bind_status_dim,'') != '' and cast(x1.bind_status_dim as int) = 2 then '通知VPN注册'
              else '未知'
         end as bind_status_name,
         case when nvl(x1.is_hi_service_dim,'') != '' and cast(x1.is_hi_service_dim as int) != -99999998 then cast(x1.is_hi_service_dim as int)
              else nvl(x1.is_hi_service,-99999998)
         end as is_hi_service,
         case when nvl(x1.is_privacy_enhance_dim,'') != '' and cast(x1.is_privacy_enhance_dim as int) != -99999998 then cast(x1.is_privacy_enhance_dim as int)
              else nvl(x1.is_privacy_enhance,-99999998)
         end as is_privacy_enhance,
         case when nvl(x1.remind_policy_id_dim,'') != '' then x1.remind_policy_id_dim
              else x1.remind_policy_id
         end as remind_policy_id,
         case when nvl(x1.sku_dim,'') != '' then x1.sku_dim
              else x1.sku
         end as sku,
         case when nvl(x1.quality_date_dim,'') != '' then x1.quality_date_dim
              else x1.quality_date
         end as quality_date,
         case when nvl(x1.customer_quality_date_dim,'') != '' then x1.customer_quality_date_dim
              else x1.customer_quality_date
         end as customer_quality_date,
         case when nvl(x1.out_type_state_dim,'') != '' and cast(x1.out_type_state_dim as int) != -99999998 then cast(x1.out_type_state_dim as int)
              else nvl(x1.out_type_state,-99999998)
         end as out_type_state,
         case when nvl(x1.out_type_state_dim,'') != '' and cast(x1.out_type_state_dim as int) = 0 then '交付出库'
              when nvl(x1.out_type_state_dim,'') != '' and cast(x1.out_type_state_dim as int) = 1 then '维修出库'
              when nvl(x1.out_type_state_dim,'') != '' and cast(x1.out_type_state_dim as int) = 2 then '报废出库'
              when nvl(x1.out_type_state_dim,'') != '' and cast(x1.out_type_state_dim as int) = 3 then '交付给客户'
              when nvl(x1.out_type_state_dim,'') != '' and cast(x1.out_type_state_dim as int) = 4 then '交付给测试'
              else '未知'
         end as out_type_state_name,
         case when nvl(x1.product_date_dim,'') != '' then x1.product_date_dim
              else x1.product_date
         end as product_date,
         case when nvl(x1.in_stock_date_dim,'') != '' then x1.in_stock_date_dim
              else x1.in_stock_date
         end as in_stock_date,
         case when nvl(x1.out_stock_date_dim,'') != '' then x1.out_stock_date_dim
              else x1.out_stock_date
         end as out_stock_date,
         case when nvl(x1.in_stock_staff_dim,'') != '' then split(x1.in_stock_staff_dim,'###')[0]
              else x1.in_stock_staff_id
         end as in_stock_staff_id,
         case when nvl(x1.in_stock_staff_dim,'') != '' then split(x1.in_stock_staff_dim,'###')[1]
              else x1.in_stock_staff_name
         end as in_stock_staff_name,
         case when nvl(x1.out_stock_staff_dim,'') != '' then split(x1.out_stock_staff_dim,'###')[0]
              else x1.out_stock_staff_id
         end as out_stock_staff_id,
         case when nvl(x1.out_stock_staff_dim,'') != '' then split(x1.out_stock_staff_dim,'###')[1]
              else x1.out_stock_staff_name
         end as out_stock_staff_name,
         case when nvl(x1.note_dim,'') != '' then x1.note_dim
              else x1.note
         end as note,
         case when nvl(x1.description_dim,'') != '' then x1.description_dim
              else x1.description
         end as description,
         x1.start_time,
         x1.end_time,
         x1.k8s_env_name,
         x1.create_time,
         x1.update_time
    from (
         select
             t1.robot_id,
             t1.robot_name,
             t1.robot_type_id,
             t1.robot_type_inner_name,
             t1.robot_type_name,
             t1.asset_code,
             t1.asset_type,
             t1.product_type_code,
             t1.product_type_code_name,
             t1.product_id,
             t1.product_id_name,
             t1.assets_status,
             t1.assets_status_name,
             t1.status,
             t1.status_name,
             t1.robot_manufacturer_id,
             t1.robot_manufacturer_name,
             t1.model,
             t1.os,
             t1.version_code,
             t1.software_version,
             t1.hardware_version,
             t1.head_url,
             t1.regist_time,
             t1.bind_status,
             t1.bind_status_name,
             t1.is_hi_service,
             t1.is_privacy_enhance,
             t1.remind_policy_id,
             t1.sku,
             t1.quality_date,
             t1.customer_quality_date,
             t1.out_type_state,
             t1.out_type_state_name,
             t1.product_date,
             t1.in_stock_date,
             t1.out_stock_date,
             t1.in_stock_staff_id,
             t1.in_stock_staff_name,
             t1.out_stock_staff_id,
             t1.out_stock_staff_name,
             t1.note,
             t1.description,
             t1.start_time,
             t1.end_time,
             t1.k8s_env_name,
             t1.create_time,
             t1.update_time,
             case when t2.robot_id is not null and size(t2.robot_name_dim) > 0 then cdmudf.dim(t1.end_time,t2.robot_name_dim)
                  else ''
             end as robot_name_dim,
             case when t2.robot_id is not null and size(t2.robot_type_dim) > 0 then cdmudf.dim(t1.end_time,t2.robot_type_dim)
                  else ''
             end as robot_type_dim,
             case when t2.robot_id is not null and size(t2.asset_code_dim) > 0 then cdmudf.dim(t1.end_time,t2.asset_code_dim)
                  else ''
             end as asset_code_dim,
             case when t2.robot_id is not null and size(t2.asset_type_dim) > 0 then cdmudf.dim(t1.end_time,t2.asset_type_dim)
                  else ''
             end as asset_type_dim,
             case when t2.robot_id is not null and size(t2.product_type_dim) > 0 then cdmudf.dim(t1.end_time,t2.product_type_dim)
                  else ''
             end as product_type_dim,
             case when t2.robot_id is not null and size(t2.product_id_dim) > 0 then cdmudf.dim(t1.end_time,t2.product_id_dim)
                  else ''
             end as product_id_dim,
             case when t2.robot_id is not null and size(t2.assets_status_dim) > 0 then cdmudf.dim(t1.end_time,t2.assets_status_dim)
                  else ''
             end as assets_status_dim,
             case when t2.robot_id is not null and size(t2.status_dim) > 0 then cdmudf.dim(t1.end_time,t2.status_dim)
                  else ''
             end as status_dim,
             case when t2.robot_id is not null and size(t2.robot_manufacturer_dim) > 0 then cdmudf.dim(t1.end_time,t2.robot_manufacturer_dim)
                  else ''
             end as robot_manufacturer_dim,
             case when t2.robot_id is not null and size(t2.model_dim) > 0 then cdmudf.dim(t1.end_time,t2.model_dim)
                  else ''
             end as model_dim,
             case when t2.robot_id is not null and size(t2.os_dim) > 0 then cdmudf.dim(t1.end_time,t2.os_dim)
                  else ''
             end as os_dim,
             case when t2.robot_id is not null and size(t2.version_code_dim) > 0 then cdmudf.dim(t1.end_time,t2.version_code_dim)
                  else ''
             end as version_code_dim,
             case when t2.robot_id is not null and size(t2.software_version_dim) > 0 then cdmudf.dim(t1.end_time,t2.software_version_dim)
                  else ''
             end as software_version_dim,
             case when t2.robot_id is not null and size(t2.hardware_version_dim) > 0 then cdmudf.dim(t1.end_time,t2.hardware_version_dim)
                  else ''
             end as hardware_version_dim,
             case when t2.robot_id is not null and size(t2.head_url_dim) > 0 then cdmudf.dim(t1.end_time,t2.head_url_dim)
                  else ''
             end as head_url_dim,
             case when t2.robot_id is not null and size(t2.regist_time_dim) > 0 then cdmudf.dim(t1.end_time,t2.regist_time_dim)
                  else ''
             end as regist_time_dim,
             case when t2.robot_id is not null and size(t2.bind_status_dim) > 0 then cdmudf.dim(t1.end_time,t2.bind_status_dim)
                  else ''
             end as bind_status_dim,
             case when t2.robot_id is not null and size(t2.is_hi_service_dim) > 0 then cdmudf.dim(t1.end_time,t2.is_hi_service_dim)
                  else ''
             end as is_hi_service_dim,
             case when t2.robot_id is not null and size(t2.is_privacy_enhance_dim) > 0 then cdmudf.dim(t1.end_time,t2.is_privacy_enhance_dim)
                  else ''
             end as is_privacy_enhance_dim,
             case when t2.robot_id is not null and size(t2.remind_policy_id_dim) > 0 then cdmudf.dim(t1.end_time,t2.remind_policy_id_dim)
                  else ''
             end as remind_policy_id_dim,
             case when t2.robot_id is not null and size(t2.sku_dim) > 0 then cdmudf.dim(t1.end_time,t2.sku_dim)
                  else ''
             end as sku_dim,
             case when t2.robot_id is not null and size(t2.quality_date_dim) > 0 then cdmudf.dim(t1.end_time,t2.quality_date_dim)
                  else ''
             end as quality_date_dim,
             case when t2.robot_id is not null and size(t2.customer_quality_date_dim) > 0 then cdmudf.dim(t1.end_time,t2.customer_quality_date_dim)
                  else ''
             end as customer_quality_date_dim,
             case when t2.robot_id is not null and size(t2.out_type_state_dim) > 0 then cdmudf.dim(t1.end_time,t2.out_type_state_dim)
                  else ''
             end as out_type_state_dim,
             case when t2.robot_id is not null and size(t2.product_date_dim) > 0 then cdmudf.dim(t1.end_time,t2.product_date_dim)
                  else ''
             end as product_date_dim,
             case when t2.robot_id is not null and size(t2.in_stock_date_dim) > 0 then cdmudf.dim(t1.end_time,t2.in_stock_date_dim)
                  else ''
             end as in_stock_date_dim,
             case when t2.robot_id is not null and size(t2.out_stock_date_dim) > 0 then cdmudf.dim(t1.end_time,t2.out_stock_date_dim)
                  else ''
             end as out_stock_date_dim,
             case when t2.robot_id is not null and size(t2.in_stock_staff_dim) > 0 then cdmudf.dim(t1.end_time,t2.in_stock_staff_dim)
                  else ''
             end as in_stock_staff_dim,
             case when t2.robot_id is not null and size(t2.out_stock_staff_dim) > 0 then cdmudf.dim(t1.end_time,t2.out_stock_staff_dim)
                  else ''
             end as out_stock_staff_dim,
             case when t2.robot_id is not null and size(t2.note_dim) > 0 then cdmudf.dim(t1.end_time,t2.note_dim)
                  else ''
             end as note_dim,
             case when t2.robot_id is not null and size(t2.description_dim) > 0 then cdmudf.dim(t1.end_time,t2.description_dim)
                  else ''
             end as description_dim
         from merge_data t1
        left join column_dim t2 on t1.robot_id = t2.robot_id
    ) x1
),
-- 再次补充制造商维度
dim_add as (
    select
        t1.robot_id,
        t1.robot_name,
        t1.robot_type_id,
        t1.robot_type_inner_name,
        t1.robot_type_name,
        t1.asset_code,
        t1.asset_type,
        t1.product_type_code,
        t1.product_type_code_name,
        t1.product_id,
        t1.product_id_name,
        t1.assets_status,
        t1.assets_status_name,
        t1.status,
        t1.status_name,
        t1.robot_manufacturer_id,
        case when t1.robot_manufacturer_name != '' then t1.robot_manufacturer_name
             else nvl(t2.robot_manufacturer_name,'')
        end as robot_manufacturer_name,
        t1.model,
        t1.os,
        t1.version_code,
        t1.software_version,
        t1.hardware_version,
        t1.head_url,
        t1.regist_time,
        t1.bind_status,
        t1.bind_status_name,
        t1.is_hi_service,
        t1.is_privacy_enhance,
        t1.remind_policy_id,
        t1.sku,
        t1.quality_date,
        t1.customer_quality_date,
        t1.out_type_state,
        t1.out_type_state_name,
        t1.product_date,
        t1.in_stock_date,
        t1.out_stock_date,
        t1.in_stock_staff_id,
        t1.in_stock_staff_name,
        t1.out_stock_staff_id,
        t1.out_stock_staff_name,
        t1.note,
        t1.description,
        t1.start_time,
        t1.end_time,
        t1.k8s_env_name,
        t1.create_time,
        t1.update_time
    from dim_repair t1
    left join crio_t_dict t2 on trim(t1.robot_manufacturer_id) = t2.robot_manufacturer_id
),
-- 使用两次排序方案，对数据去重保持状态延续
-- 先按照robot_id分组,end_time升序排序
-- 然后按照robot_id,关键属性md5分组,end_time升序排序
-- 分组后的数值做差,按照robot_id,关键属性md5,差值分组,组内更新end_time时间,返回更新后的记录即可
state_continue as (
    select
        robot_id,
        robot_name,
        robot_type_id,
        robot_type_inner_name,
        robot_type_name,
        asset_code,
        asset_type,
        product_type_code,
        product_type_code_name,
        product_id,
        product_id_name,
        assets_status,
        assets_status_name,
        status,
        status_name,
        robot_manufacturer_id,
        robot_manufacturer_name,
        model,
        os,
        version_code,
        software_version,
        hardware_version,
        head_url,
        regist_time,
        bind_status,
        bind_status_name,
        is_hi_service,
        is_privacy_enhance,
        remind_policy_id,
        sku,
        quality_date,
        customer_quality_date,
        out_type_state,
        out_type_state_name,
        product_date,
        in_stock_date,
        out_stock_date,
        in_stock_staff_id,
        in_stock_staff_name,
        out_stock_staff_id,
        out_stock_staff_name,
        note,
        description,
        min(start_time) as start_time,
        max(end_time) as end_time,
        k8s_env_name,
        create_time,
        min(update_time) as update_time,
        row_number() OVER (PARTITION BY robot_id ORDER BY create_time, min(update_time) asc) as rnk
    from (
        select
            robot_id,
            robot_name,
            robot_type_id,
            robot_type_inner_name,
            robot_type_name,
            asset_code,
            asset_type,
            product_type_code,
            product_type_code_name,
            product_id,
            product_id_name,
            assets_status,
            assets_status_name,
            status,
            status_name,
            robot_manufacturer_id,
            robot_manufacturer_name,
            model,
            os,
            version_code,
            software_version,
            hardware_version,
            head_url,
            regist_time,
            bind_status,
            bind_status_name,
            is_hi_service,
            is_privacy_enhance,
            remind_policy_id,
            sku,
            quality_date,
            customer_quality_date,
            out_type_state,
            out_type_state_name,
            product_date,
            in_stock_date,
            out_stock_date,
            in_stock_staff_id,
            in_stock_staff_name,
            out_stock_staff_id,
            out_stock_staff_name,
            note,
            description,
            start_time,
            end_time,
            k8s_env_name,
            create_time,
            update_time,
            row_number() over(partition by robot_id order by end_time asc) as rnk1,
            row_number() over(partition by robot_id,robot_name,robot_type_id,robot_type_inner_name,robot_type_name,asset_code,asset_type,product_type_code,product_type_code_name,product_id,product_id_name,assets_status,assets_status_name,status,status_name,robot_manufacturer_id,robot_manufacturer_name,model,os,version_code,software_version,hardware_version,head_url,regist_time,bind_status,bind_status_name,is_hi_service,is_privacy_enhance,remind_policy_id,sku,quality_date,customer_quality_date,out_type_state,out_type_state_name,product_date,in_stock_date,out_stock_date,in_stock_staff_id,in_stock_staff_name,out_stock_staff_id,out_stock_staff_name,note,description,create_time,k8s_env_name order by end_time asc) as rnk2
        from dim_add
    ) t group by robot_id,robot_name,robot_type_id,robot_type_inner_name,robot_type_name,asset_code,asset_type,product_type_code,product_type_code_name,product_id,product_id_name,assets_status,assets_status_name,status,status_name,robot_manufacturer_id,robot_manufacturer_name,model,os,version_code,software_version,hardware_version,head_url,regist_time,bind_status,bind_status_name,is_hi_service,is_privacy_enhance,remind_policy_id,sku,quality_date,customer_quality_date,out_type_state,out_type_state_name,product_date,in_stock_date,out_stock_date,in_stock_staff_id,in_stock_staff_name,out_stock_staff_id,out_stock_staff_name,note,description,create_time,k8s_env_name,rnk2-rnk1
),
-- 首条记录保持 使用create_time作为start_time
modify_start_time as (
    select
       robot_id,
        robot_name,
        robot_type_id,
        robot_type_inner_name,
        robot_type_name,
        asset_code,
        asset_type,
        product_type_code,
        product_type_code_name,
        product_id,
        product_id_name,
        assets_status,
        assets_status_name,
        status,
        status_name,
        robot_manufacturer_id,
        robot_manufacturer_name,
        model,
        os,
        version_code,
        software_version,
        hardware_version,
        head_url,
        regist_time,
        bind_status,
        bind_status_name,
        is_hi_service,
        is_privacy_enhance,
        remind_policy_id,
        sku,
        quality_date,
        customer_quality_date,
        out_type_state,
        out_type_state_name,
        product_date,
        in_stock_date,
        out_stock_date,
        in_stock_staff_id,
        in_stock_staff_name,
        out_stock_staff_id,
        out_stock_staff_name,
        note,
        description,
        case when rnk = 1 then create_time
             else start_time
        end as start_time,
        end_time,
        k8s_env_name,
        create_time,
        update_time
    from state_continue
)
insert overwrite table cdmdim.dim_pmd_robot_sh_d partition(dt)
    select
        robot_id,
        robot_name,
        robot_type_id,
        robot_type_inner_name,
        robot_type_name,
        asset_code,
        asset_type,
        product_type_code,
        product_type_code_name,
        product_id,
        product_id_name,
        assets_status,
        assets_status_name,
        status,
        status_name,
        robot_manufacturer_id,
        robot_manufacturer_name,
        model,
        os,
        version_code,
        software_version,
        hardware_version,
        head_url,
        regist_time,
        bind_status,
        bind_status_name,
        is_hi_service,
        is_privacy_enhance,
        remind_policy_id,
        sku,
        quality_date,
        customer_quality_date,
        out_type_state,
        out_type_state_name,
        product_date,
        in_stock_date,
        out_stock_date,
        in_stock_staff_id,
        in_stock_staff_name,
        out_stock_staff_id,
        out_stock_staff_name,
        note,
        description,
        start_time,
        end_time,
        k8s_env_name,
        create_time,
        update_time,
        '${e_dt_var}' as dt
    from modify_start_time;


-- boss新整合的数据
with roc_t_dict as (
    select
        t1.robot_type_id,
        t1.robot_type_name,
        t2.robot_type_inner_name
    from (
         select
             label as robot_type_name,
             `value` as robot_type_id
         from cdmods.ods_roc_db_c0002_t_dict_s_d
         where dt = '${e_dt_var}' and TYPE = 'robot-type'
         group by label, `value`
    ) t1
    full join (
        select
            label as robot_type_inner_name,
            `value` as robot_type_id
        from cdmods.ods_roc_db_c0002_t_dict_s_d
        where dt = '${e_dt_var}' and TYPE = 'robot-type-hari'
        group by label, `value`
    ) t2 on t1.robot_type_id = t2.robot_type_id
),
-- 机器人制造商表 type=2
crio_t_dict as (
    select
        trim(code) as robot_manufacturer_id,
        trim(name) as robot_manufacturer_name
    from cdmods.ods_crio_db_c0001_t_dict_s_d where dt = '${e_dt_var}' and `type` = 2
),
crio_t_device as (
    select
        x1.robot_id,
        x1.robot_name,
        x1.robot_type_id,
        nvl(x2.robot_type_inner_name,'') as robot_type_inner_name,
        x1.robot_type_name as robot_type_name,
        x1.tenant_id,
        x1.rcu_id,
        x1.robot_account_id,
        x1.robot_account_name,
        x1.asset_code,
        x1.asset_type,
        x1.asset_type_name,
        x1.product_type_code,
        x1.product_type_code_name,
        x1.product_id,
        x1.product_id_name,
        x1.asset_status,
        x1.asset_status_name,
        x1.status,
        x1.status_name,
        x1.roc_status,
        x1.roc_status_name,
        nvl(x1.robot_manufacturer_id,'') as robot_manufacturer_id,
        case when nvl(x3.robot_manufacturer_name,'') != '' then x3.robot_manufacturer_name
             else nvl(x1.robot_manufacturer_name,'')
            end as robot_manufacturer_name,
        x1.model,
        x1.os,
        x1.version_code,
        x1.software_version,
        x1.hardware_version,
        x1.head_url,
        x1.regist_time,
        x1.bind_status,
        x1.bind_status_name,
        x1.is_hi_service,
        x1.is_privacy_enhance,
        x1.remind_policy_id,
        x1.sku,
        x1.quality_date,
        x1.customer_quality_date,
        x1.product_date,
        x1.roc_delivery_status,
        x1.roc_delivery_status_name,
        x1.is_special_asset,
        x1.serial_number,
        x1.operating_status,
        x1.operating_status_name,
        x1.running_status,
        x1.running_status_name,
        x1.environment,
        x1.description,
        x1.create_time,
        x1.update_time,
        x1.k8s_env_name
    from (
         select
             nvl(t1.device_code,'') as robot_id,
             nvl(t1.device_name,'') as robot_name,
             nvl(t2.robot_type_id,'') as robot_type_id,
             nvl(t2.robot_type_name,'') as robot_type_name,
             nvl(t1.tenant_code,'') as tenant_id,
             '' as rcu_id,
             '' as robot_account_id,
             '' as robot_account_name,
             nvl(t1.asset_code,'') as asset_code,
             nvl(t1.asset_type,'001') as asset_type,
             case when trim(t1.asset_type) = '1' then '达闼固资'
                  when trim(t1.asset_type) = '2' then '达闼存资'
                  when trim(t1.asset_type) = '3' then '客户资产'
                  else '达闼固资'
                 end as asset_type_name,
             nvl(t1.product_type_code,'') as product_type_code,
             nvl(t1.product_type_code_name,'') as product_type_code_name,
             nvl(t1.product_id,'') as product_id,
             nvl(t1.product_id_name,'') as product_id_name,
             nvl(t1.asset_status,-99999998) as asset_status,
             case when t1.asset_status = 1 then '在册'
                  when t1.asset_status = 2 then '待测'
                  when t1.asset_status = 3 then '研发测试'
                  when t1.asset_status = 4 then '空闲'
                  when t1.asset_status = 5 then '项目中'
                  else '未知'
                 end as asset_status_name,
             case when t1.status = 1 then 1
                  when t1.status = 9 then 9
                  else -99999998
                 end as status,
             case when t1.status = 1 then '正常'
                  when t1.status = 9 then '删除'
                  else '未知'
                 end as status_name,
             -99999998 as roc_status,
             '未知' as roc_status_name,
             nvl(supplier_code,'') as robot_manufacturer_id,
             nvl(supplier_code_name,'') as robot_manufacturer_name,
             nvl(t1.device_model,'') as model,
             '' as os,
             '' as version_code,
             nvl(t1.software_version,'') as software_version,
             nvl(t1.hardware_version,'') as hardware_version,
             '' as head_url,
             '' as regist_time,
             -99999998 as bind_status,
             '未知' as bind_status_name,
             -99999998 as is_hi_service,
             -99999998 as is_privacy_enhance,
             '' as remind_policy_id,
             nvl(t1.sku,'') as sku,
             case when length(t1.quality_date) = 10 then nvl(concat(from_unixtime( cast(t1.quality_date as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
                  when length(t1.quality_date) = 19 then nvl(concat(from_utc_timestamp(t1.quality_date,'PRC'),'.000'),'')
                  when length(t1.quality_date) > 19 and length(t1.quality_date) < 23 then nvl(rpad(from_utc_timestamp(t1.quality_date,'PRC'),23,'0'),'')
                  when length(t1.quality_date) >= 23 then nvl(from_utc_timestamp(SUBSTRING(REGEXP_REPLACE(t1.quality_date,'T',' '),0,23),'PRC'),'')
--                   when length(t1.quality_date) = 19 then nvl(concat(t1.quality_date,'.000'),'')
--                   when length(t1.quality_date) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(t1.quality_date,'T',' '),0,23),'')
                  else nvl(concat(from_unixtime( cast(t1.quality_date/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(t1.quality_date,11,3)),'')
             end as quality_date,
             case when length(t1.customer_quality_date) = 10 then nvl(concat(from_unixtime( cast(t1.customer_quality_date as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
                  when length(t1.customer_quality_date) = 19 then nvl(concat(from_utc_timestamp(t1.customer_quality_date,'PRC'),'.000'),'')
                  when length(t1.customer_quality_date) > 19 and length(t1.customer_quality_date) < 23 then nvl(rpad(from_utc_timestamp(t1.customer_quality_date,'PRC'),23,'0'),'')
                  when length(t1.customer_quality_date) >= 23 then nvl(from_utc_timestamp(SUBSTRING(REGEXP_REPLACE(t1.customer_quality_date,'T',' '),0,23),'PRC'),'')
--                   when length(t1.customer_quality_date) = 19 then nvl(concat(t1.customer_quality_date,'.000'),'')
--                   when length(t1.customer_quality_date) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(t1.customer_quality_date,'T',' '),0,23),'')
                  else nvl(concat(from_unixtime( cast(t1.customer_quality_date/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(t1.customer_quality_date,11,3)),'')
             end as customer_quality_date,
             case when length(t1.product_date) = 10 then nvl(concat(from_unixtime( cast(t1.product_date as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
                  when length(t1.product_date) = 19 then nvl(concat(from_utc_timestamp(t1.product_date,'PRC'),'.000'),'')
                  when length(t1.product_date) > 19 and length(t1.product_date) < 23 then nvl(rpad(from_utc_timestamp(t1.product_date,'PRC'),23,'0'),'')
                  when length(t1.product_date) >= 23 then nvl(from_utc_timestamp(SUBSTRING(REGEXP_REPLACE(t1.product_date,'T',' '),0,23),'PRC'),'')
--                   when length(t1.product_date) = 19 then nvl(concat(t1.product_date,'.000'),'')
--                   when length(t1.product_date) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(t1.product_date,'T',' '),0,23),'')
                  else nvl(concat(from_unixtime( cast(t1.product_date/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(t1.product_date,11,3)),'')
             end as product_date,
             nvl(t1.roc_delivery_status,1) as roc_delivery_status,
             case when t1.roc_delivery_status = 0 then '已交付'
                  when t1.roc_delivery_status = 1 then '未交付'
                  when t1.roc_delivery_status = 2 then '交付中'
                  when t1.roc_delivery_status = 3 then '回收中'
                  else '未交付'
             end as roc_delivery_status_name,
             case when t1.is_special = 1 then 1
                  else 0
             end as is_special_asset,
             nvl(t1.serial_number,'') as serial_number,
             nvl(t1.operating_status,1) as operating_status,
             case when t1.operating_status = 1 then '空闲'
                  when t1.operating_status = 2 then '测试中'
                  when t1.operating_status = 3 then '演示中'
                  when t1.operating_status = 4 then '运营中'
                  when t1.operating_status = 5 then '交付中'
                  else '未知'
             end as operating_status_name,
             nvl(t1.running_status,1) as running_status,
             case when t1.running_status = 1 then '良好'
                  when t1.running_status = 2 then '故障'
                  when t1.running_status = 3 then '维修中'
                  else '未知'
             end as running_status_name,
             nvl(environment,'') as environment,
             '' as description,
             case when length(t1.create_time) = 10 then nvl(concat(from_unixtime( cast(t1.create_time as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
                  when length(t1.create_time) = 19 then nvl(concat(from_utc_timestamp(t1.create_time,'PRC'),'.000'),'')
                  when length(t1.create_time) > 19 and length(t1.create_time) < 23 then nvl(rpad(from_utc_timestamp(t1.create_time,'PRC'),23,'0'),'')
                  when length(t1.create_time) >= 23 then nvl(from_utc_timestamp(SUBSTRING(REGEXP_REPLACE(t1.create_time,'T',' '),0,23),'PRC'),'')
--                   when length(t1.create_time) = 19 then nvl(concat(t1.create_time,'.000'),'')
--                   when length(t1.create_time) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(t1.create_time,'T',' '),0,23),'')
                  else nvl(concat(from_unixtime( cast(t1.create_time/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(t1.create_time,11,3)),'')
             end as create_time,
             case when length(t1.update_time) = 10 then nvl(concat(from_unixtime( cast(t1.update_time as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
                  when length(t1.update_time) = 19 then nvl(concat(from_utc_timestamp(t1.update_time,'PRC'),'.000'),'')
                  when length(t1.create_time) > 19 and length(t1.create_time) < 23 then nvl(rpad(from_utc_timestamp(t1.create_time,'PRC'),23,'0'),'')
                  when length(t1.update_time) >= 23 then nvl(from_utc_timestamp(SUBSTRING(REGEXP_REPLACE(t1.update_time,'T',' '),0,23),'PRC'),'')
--                   when length(t1.update_time) = 19 then nvl(concat(t1.update_time,'.000'),'')
--                   when length(t1.update_time) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(t1.update_time,'T',' '),0,23),'')
                  else nvl(concat(from_unixtime( cast(t1.update_time/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(t1.update_time,11,3)),'')
             end as update_time,
             'bj-prod-232' as k8s_env_name
        from cdmods.ods_crio_db_c0001_t_device_i_d t1
        left join (
            select
                robot_type_id,
                robot_type_name
            from roc_t_dict
        ) t2 on trim(lower(regexp_replace(t1.product_type_code,'\\s',''))) = trim(lower(regexp_replace(t2.robot_type_name,'\\s','')))
        where dt >= '${s_dt_var}' and dt <= '${e_dt_var}'
    ) x1
    left join (
        select
            robot_type_id,
            robot_type_inner_name
        from roc_t_dict
    ) x2 on x1.robot_type_id = x2.robot_type_id
    left join crio_t_dict x3 on nvl(trim(x1.robot_manufacturer_id),'') = x3.robot_manufacturer_id
    where substring(x1.update_time,0,10) >= '${s_dt_var}' and substring(x1.update_time,0,10) <= '${e_dt_var}'
),
roc_t_robot as (
    select
        t1.robot_id,
        t1.robot_name,
        t1.robot_type_id,
        nvl(t2.robot_type_inner_name,'') as robot_type_inner_name,
        nvl(t2.robot_type_name,'') as robot_type_name,
        t1.tenant_id,
        t1.rcu_id,
        t1.robot_account_id,
        t1.robot_account_name,
        t1.asset_code,
        t1.asset_type,
        t1.asset_type_name,
        t1.product_type_code,
        t1.product_type_code_name,
        t1.product_id,
        t1.product_id_name,
        t1.asset_status,
        t1.asset_status_name,
        t1.status,
        t1.status_name,
        t1.roc_status,
        t1.roc_status_name,
        nvl(t1.robot_manufacturer_id,'') as robot_manufacturer_id,
        case when nvl(t3.robot_manufacturer_name,'') != '' then t3.robot_manufacturer_name
             else nvl(t1.robot_manufacturer_name,'')
        end as robot_manufacturer_name,
        t1.model,
        t1.os,
        t1.version_code,
        t1.software_version,
        t1.hardware_version,
        t1.head_url,
        t1.regist_time,
        t1.bind_status,
        t1.bind_status_name,
        t1.is_hi_service,
        t1.is_privacy_enhance,
        t1.remind_policy_id,
        t1.sku,
        t1.quality_date,
        t1.customer_quality_date,
        t1.product_date,
        t1.roc_delivery_status,
        t1.roc_delivery_status_name,
        t1.is_special_asset,
        t1.serial_number,
        t1.operating_status,
        t1.operating_status_name,
        t1.running_status,
        t1.running_status_name,
        t1.environment,
        t1.description,
        t1.create_time,
        t1.update_time,
        t1.k8s_env_name
    from (
         select
             nvl(robot_code,'') as robot_id,
             nvl(robot_name,'') as robot_name,
             cast(nvl(robot_type,'') as string) as robot_type_id,
             nvl(tenant_code,'') as tenant_id,
             nvl(rcu_code,'') as rcu_id,
             nvl(user_code,'') as robot_account_id,
             nvl(user_name,'') as robot_account_name,
             '' as asset_code,
             '' as asset_type,
             '' as asset_type_name,
             '' as product_type_code,
             '' as product_type_code_name,
             '' as product_id,
             '' as product_id_name,
             -99999998 as asset_status,
             '未知' as asset_status_name,
             -99999998 as status,
             '未知' as status_name,
             nvl(status,-99999998) as roc_status,
             case when status = 0 then '正常'
                  when status = 1 then '停用'
                  when status = -1 then '删除'
                  when status = 2 then '待删除'
                  else '未知'
             end as roc_status_name,
             manufacturer as robot_manufacturer_id,
             '' as robot_manufacturer_name,
             nvl(model,'') as model,
             nvl(os,'') as os,
             nvl(version_code,'') as version_code,
             '' as software_version,
             '' as hardware_version,
             nvl(head_url,'') as head_url,
             nvl(regist_time,'') as regist_time,
             nvl(bind_status,-99999998) as bind_status,
             case when bind_status = -1 then '删除'
                  when bind_status = 0 then '注册未激活'
                  when bind_status = 1 then '激活可用'
                  when bind_status = 2 then '通知VPN注册'
                  else '未知'
             end as bind_status_name,
             nvl(hi_service,-99999998) as is_hi_service,
             nvl(privacy_enhance,-99999998) as is_privacy_enhance,
             nvl(remind_policy_id,'') as remind_policy_id,
             nvl(sku,'') as sku,
             '' as quality_date,
             '' as customer_quality_date,
             '' as product_date,
             -99999998 as roc_delivery_status,
             '未知' as roc_delivery_status_name,
             1 as is_special_asset,
             '' as serial_number,
             -99999998 as operating_status,
             '未知' as operating_status_name,
             -99999998 as running_status,
             '未知' as running_status_name,
             '' as environment,
             regexp_replace(nvl(trim(description),''), '\r|\n|\t', '') as description,
             case when length(regist_time) = 10 then nvl(concat(from_unixtime( cast(regist_time as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
                  when length(regist_time) = 19 then nvl(concat(regist_time,'.000'),'')
                  when length(regist_time) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(regist_time,'T',' '),0,23),'')
                  else nvl(concat(from_unixtime( cast(regist_time/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(regist_time,11,3)),'')
             end as create_time,
             case when length(update_time) = 10 then nvl(concat(from_unixtime( cast(update_time as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
                  when length(update_time) = 19 then nvl(concat(update_time,'.000'),'')
                  when length(update_time) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(update_time,'T',' '),0,23),'')
                  else nvl(concat(from_unixtime( cast(update_time/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(update_time,11,3)),'')
             end as update_time,
             k8s_env_name
         from cdmods.ods_roc_db_c0002_t_robot_i_d where dt >= '${s_dt_var}' and dt <= '${e_dt_var}'
         union all
         select
             nvl(robot_code,'') as robot_id,
             nvl(robot_name,'') as robot_name,
             cast(nvl(robot_type,'') as string) as robot_type_id,
             nvl(tenant_code,'') as tenant_id,
             nvl(rcu_code,'') as rcu_id,
             nvl(user_code,'') as robot_account_id,
             nvl(user_name,'') as robot_account_name,
             '' as asset_code,
             '' as asset_type,
             '' as asset_type_name,
             '' as product_type_code,
             '' as product_type_code_name,
             '' as product_id,
             '' as product_id_name,
             -99999998 as asset_status,
             '未知' as asset_status_name,
             -99999998 as status,
             '未知' as status_name,
             nvl(status,-99999998) as roc_status,
             case when status = 0 then '正常'
                  when status = 1 then '停用'
                  when status = -1 then '删除'
                  when status = 2 then '待删除'
                  else '未知'
             end as roc_status_name,
             manufacturer as robot_manufacturer_id,
             '' as robot_manufacturer_name,
             nvl(model,'') as model,
             nvl(os,'') as os,
             nvl(version_code,'') as version_code,
             '' as software_version,
             '' as hardware_version,
             nvl(head_url,'') as head_url,
             nvl(regist_time,'') as regist_time,
             nvl(bind_status,-99999998) as bind_status,
             case when bind_status = -1 then '删除'
                  when bind_status = 0 then '注册未激活'
                  when bind_status = 1 then '激活可用'
                  when bind_status = 2 then '通知VPN注册'
                  else '未知'
             end as bind_status_name,
             nvl(hi_service,-99999998) as is_hi_service,
             nvl(privacy_enhance,-99999998) as is_privacy_enhance,
             nvl(remind_policy_id,'') as remind_policy_id,
             nvl(sku,'') as sku,
             '' as quality_date,
             '' as customer_quality_date,
             '' as product_date,
             -99999998 as roc_delivery_status,
             '未知' as roc_delivery_status_name,
             1 as is_special_asset,
             '' as serial_number,
             -99999998 as operating_status,
             '未知' as operating_status_name,
             -99999998 as running_status,
             '未知' as running_status_name,
             '' as environment,
             regexp_replace(nvl(trim(description),''), '\r|\n|\t', '') as description,
             case when length(regist_time) = 10 then nvl(concat(from_unixtime( cast(regist_time as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
                  when length(regist_time) = 19 then nvl(concat(regist_time,'.000'),'')
                  when length(regist_time) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(regist_time,'T',' '),0,23),'')
                  else nvl(concat(from_unixtime( cast(regist_time/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(regist_time,11,3)),'')
             end as create_time,
             case when length(update_time) = 10 then nvl(concat(from_unixtime( cast(update_time as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
                  when length(update_time) = 19 then nvl(concat(update_time,'.000'),'')
                  when length(update_time) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(update_time,'T',' '),0,23),'')
                  else nvl(concat(from_unixtime( cast(update_time/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(update_time,11,3)),'')
             end as update_time,
             k8s_env_name
         from cdmods.ods_roc_db_c0008_t_robot_i_d where dt >= '${s_dt_var}' and dt <= '${e_dt_var}') t1
        left join (
            select
                robot_type_id,
                robot_type_name,
                robot_type_inner_name
            from roc_t_dict
    ) t2 on t1.robot_type_id = t2.robot_type_id
    left join crio_t_dict t3 on nvl(trim(t1.robot_manufacturer_id),'') = t3.robot_manufacturer_id
    where substring(t1.update_time,0,10) >= '${s_dt_var}' and substring(t1.update_time,0,10) <= '${e_dt_var}'
),
remove_duplicate_robot as (
    select
        robot_id,
        robot_name,
        robot_type_id,
        robot_type_inner_name,
        robot_type_name,
        tenant_id,
        rcu_id,
        robot_account_id,
        robot_account_name,
        asset_code,
        asset_type,
        asset_type_name,
        product_type_code,
        product_type_code_name,
        product_id,
        product_id_name,
        asset_status,
        asset_status_name,
        status,
        status_name,
        roc_status,
        roc_status_name,
        robot_manufacturer_id,
        robot_manufacturer_name,
        model,
        os,
        version_code,
        software_version,
        hardware_version,
        head_url,
        regist_time,
        bind_status,
        bind_status_name,
        is_hi_service,
        is_privacy_enhance,
        remind_policy_id,
        sku,
        quality_date,
        customer_quality_date,
        product_date,
        roc_delivery_status,
        roc_delivery_status_name,
        is_special_asset,
        serial_number,
        operating_status,
        operating_status_name,
        running_status,
        running_status_name,
        environment,
        description,
        create_time,
        update_time,
        k8s_env_name
    from (
         select
             robot_id,
             robot_name,
             robot_type_id,
             robot_type_inner_name,
             robot_type_name,
             tenant_id,
             rcu_id,
             robot_account_id,
             robot_account_name,
             asset_code,
             asset_type,
             asset_type_name,
             product_type_code,
             product_type_code_name,
             product_id,
             product_id_name,
             asset_status,
             asset_status_name,
             status,
             status_name,
             roc_status,
             roc_status_name,
             robot_manufacturer_id,
             robot_manufacturer_name,
             model,
             os,
             version_code,
             software_version,
             hardware_version,
             head_url,
             regist_time,
             bind_status,
             bind_status_name,
             is_hi_service,
             is_privacy_enhance,
             remind_policy_id,
             sku,
             quality_date,
             customer_quality_date,
             product_date,
             roc_delivery_status,
             roc_delivery_status_name,
             is_special_asset,
             serial_number,
             operating_status,
             operating_status_name,
             running_status,
             running_status_name,
             environment,
             description,
             create_time,
             update_time,
             k8s_env_name,
             row_number() OVER (PARTITION BY robot_id,robot_name,robot_type_id,robot_type_inner_name,robot_type_name,tenant_id,rcu_id,robot_account_id,robot_account_name,asset_code,asset_type,product_type_code,product_type_code_name,product_id,product_id_name,asset_status,asset_status_name,status,status_name,roc_status,roc_status_name,robot_manufacturer_id,robot_manufacturer_name,model,os,version_code,software_version,hardware_version,head_url,regist_time,bind_status,bind_status_name,is_hi_service,is_privacy_enhance,remind_policy_id,sku,quality_date,customer_quality_date,product_date,roc_delivery_status,roc_delivery_status_name,is_special_asset,serial_number,operating_status,operating_status_name,running_status,running_status_name,environment,description,create_time,update_time,k8s_env_name ORDER BY update_time DESC) as rnk
         from crio_t_device
         union all
         select
             robot_id,
             robot_name,
             robot_type_id,
             robot_type_inner_name,
             robot_type_name,
             tenant_id,
             rcu_id,
             robot_account_id,
             robot_account_name,
             asset_code,
             asset_type,
             asset_type_name,
             product_type_code,
             product_type_code_name,
             product_id,
             product_id_name,
             asset_status,
             asset_status_name,
             status,
             status_name,
             roc_status,
             roc_status_name,
             robot_manufacturer_id,
             robot_manufacturer_name,
             model,
             os,
             version_code,
             software_version,
             hardware_version,
             head_url,
             regist_time,
             bind_status,
             bind_status_name,
             is_hi_service,
             is_privacy_enhance,
             remind_policy_id,
             sku,
             quality_date,
             customer_quality_date,
             product_date,
             roc_delivery_status,
             roc_delivery_status_name,
             is_special_asset,
             serial_number,
             operating_status,
             operating_status_name,
             running_status,
             running_status_name,
             environment,
             description,
             create_time,
             update_time,
             k8s_env_name,
             row_number() OVER (PARTITION BY robot_id,robot_name,robot_type_id,robot_type_inner_name,robot_type_name,tenant_id,rcu_id,robot_account_id,robot_account_name,asset_code,asset_type,product_type_code,product_type_code_name,product_id,product_id_name,asset_status,asset_status_name,status,status_name,roc_status,roc_status_name,robot_manufacturer_id,robot_manufacturer_name,model,os,version_code,software_version,hardware_version,head_url,regist_time,bind_status,bind_status_name,is_hi_service,is_privacy_enhance,remind_policy_id,sku,quality_date,customer_quality_date,product_date,roc_delivery_status,roc_delivery_status_name,is_special_asset,serial_number,operating_status,operating_status_name,running_status,running_status_name,environment,description,k8s_env_name ORDER BY create_time asc, update_time DESC) as rnk
         from roc_t_robot
    ) t1 where t1.rnk = 1
),
merge_data as (
    select
        x1.robot_id,
        x1.robot_name,
        x1.robot_type_id,
        x1.robot_type_inner_name,
        x1.robot_type_name,
        x1.tenant_id,
        x1.rcu_id,
        x1.robot_account_id,
        x1.robot_account_name,
        x1.asset_code,
        x1.asset_type,
        x1.asset_type_name,
        x1.product_type_code,
        x1.product_type_code_name,
        x1.product_id,
        x1.product_id_name,
        x1.asset_status,
        x1.asset_status_name,
        x1.status,
        x1.status_name,
        x1.roc_status,
        x1.roc_status_name,
        x1.robot_manufacturer_id,
        x1.robot_manufacturer_name,
        x1.model,
        x1.os,
        x1.version_code,
        x1.software_version,
        x1.hardware_version,
        x1.head_url,
        x1.regist_time,
        x1.bind_status,
        x1.bind_status_name,
        x1.is_hi_service,
        x1.is_privacy_enhance,
        x1.remind_policy_id,
        x1.sku,
        x1.quality_date,
        x1.customer_quality_date,
        x1.product_date,
        x1.roc_delivery_status,
        x1.roc_delivery_status_name,
        x1.is_special_asset,
        x1.serial_number,
        x1.operating_status,
        x1.operating_status_name,
        x1.running_status,
        x1.running_status_name,
        x1.environment,
        x1.description,
        case when x1.update_time != '' and x1.update_time is not null then x1.update_time
             else ''
        end as start_time,
        CASE WHEN tmp_end_time IS NOT NULL AND tmp_end_time != '' THEN concat(from_unixtime(cast((cast(unix_timestamp(substring(tmp_end_time,0,19),'yyyy-MM-dd HH:mm:ss') AS bigint)*1000 + cast(substring(tmp_end_time,21,23) AS bigint) - 1)/1000 as bigint),'yyyy-MM-dd HH:mm:ss'),'.',SUBSTRING((cast(unix_timestamp(substring(tmp_end_time,0,19),'yyyy-MM-dd HH:mm:ss') AS bigint)*1000 + cast(substring(tmp_end_time,21,23) AS bigint) - 1),11,13))
             ELSE '9999-12-31 23:59:59.999'
        END AS end_time,
        x1.k8s_env_name,
        x1.create_time,
        x1.update_time,
        first_value(create_time,true) over (partition by robot_id order by create_time asc nulls last range between unbounded preceding and unbounded following) as tmp_create_time
    from (
         select
             t1.robot_id,
             t1.robot_name,
             t1.robot_type_id,
             t1.robot_type_inner_name,
             t1.robot_type_name,
             t1.tenant_id,
             t1.rcu_id,
             t1.robot_account_id,
             t1.robot_account_name,
             t1.asset_code,
             t1.asset_type,
             t1.asset_type_name,
             t1.product_type_code,
             t1.product_type_code_name,
             t1.product_id,
             t1.product_id_name,
             t1.asset_status,
             t1.asset_status_name,
             t1.status,
             t1.status_name,
             t1.roc_status,
             t1.roc_status_name,
             t1.robot_manufacturer_id,
             t1.robot_manufacturer_name,
             t1.model,
             t1.os,
             t1.version_code,
             t1.software_version,
             t1.hardware_version,
             t1.head_url,
             t1.regist_time,
             t1.bind_status,
             t1.bind_status_name,
             t1.is_hi_service,
             t1.is_privacy_enhance,
             t1.remind_policy_id,
             t1.sku,
             t1.quality_date,
             t1.customer_quality_date,
             t1.product_date,
             t1.roc_delivery_status,
             t1.roc_delivery_status_name,
             t1.is_special_asset,
             t1.serial_number,
             t1.operating_status,
             t1.operating_status_name,
             t1.running_status,
             t1.running_status_name,
             t1.environment,
             t1.description,
             t1.start_time,
             t1.end_time,
             t1.k8s_env_name,
             t1.create_time,
             t1.update_time,
             lead(t1.update_time,1,NULL) over(partition BY t1.robot_id ORDER BY t1.update_time ASC) as tmp_end_time
         from (
              select
                  robot_id,
                  robot_name,
                  robot_type_id,
                  robot_type_inner_name,
                  robot_type_name,
                  tenant_id,
                  rcu_id,
                  robot_account_id,
                  robot_account_name,
                  asset_code,
                  asset_type,
                  asset_type_name,
                  product_type_code,
                  product_type_code_name,
                  product_id,
                  product_id_name,
                  asset_status,
                  asset_status_name,
                  status,
                  status_name,
                  roc_status,
                  roc_status_name,
                  robot_manufacturer_id,
                  robot_manufacturer_name,
                  model,
                  os,
                  version_code,
                  software_version,
                  hardware_version,
                  head_url,
                  regist_time,
                  bind_status,
                  bind_status_name,
                  is_hi_service,
                  is_privacy_enhance,
                  remind_policy_id,
                  sku,
                  quality_date,
                  customer_quality_date,
                  product_date,
                  roc_delivery_status,
                  roc_delivery_status_name,
                  is_special_asset,
                  serial_number,
                  operating_status,
                  operating_status_name,
                  running_status,
                  running_status_name,
                  environment,
                  description,
                  start_time,
                  end_time,
                  k8s_env_name,
                  create_time,
                  update_time
              from cdmdim.dim_pmd_robot_sh_d
              where dt = date_sub('${e_dt_var}',1)
              union all
              select
                  robot_id,
                  robot_name,
                  robot_type_id,
                  robot_type_inner_name,
                  robot_type_name,
                  tenant_id,
                  rcu_id,
                  robot_account_id,
                  robot_account_name,
                  asset_code,
                  asset_type,
                  asset_type_name,
                  product_type_code,
                  product_type_code_name,
                  product_id,
                  product_id_name,
                  asset_status,
                  asset_status_name,
                  status,
                  status_name,
                  roc_status,
                  roc_status_name,
                  robot_manufacturer_id,
                  robot_manufacturer_name,
                  model,
                  os,
                  version_code,
                  software_version,
                  hardware_version,
                  head_url,
                  regist_time,
                  bind_status,
                  bind_status_name,
                  is_hi_service,
                  is_privacy_enhance,
                  remind_policy_id,
                  sku,
                  quality_date,
                  customer_quality_date,
                  product_date,
                  roc_delivery_status,
                  roc_delivery_status_name,
                  is_special_asset,
                  serial_number,
                  operating_status,
                  operating_status_name,
                  running_status,
                  running_status_name,
                  environment,
                  description,
                  '' as start_time,
                  '' as end_time,
                  k8s_env_name,
                  create_time,
                  update_time
              from remove_duplicate_robot
          ) t1
    ) x1
),
-- 二次排序，选择上一次非删除状态的所有有效状态数据
-- group_by_roc_status as (
--     select
--         robot_id,
--         robot_name,
--         robot_type_id,
--         robot_type_inner_name,
--         robot_type_name,
--         tenant_id,
--         rcu_id,
--         robot_account_id,
--         robot_account_name,
--         asset_code,
--         asset_type,
--         asset_type_name,
--         product_type_code,
--         product_type_code_name,
--         product_id,
--         product_id_name,
--         asset_status,
--         asset_status_name,
--         status,
--         status_name,
--         roc_status,
--         roc_status_name,
--         robot_manufacturer_id,
--         robot_manufacturer_name,
--         model,
--         os,
--         version_code,
--         software_version,
--         hardware_version,
--         head_url,
--         regist_time,
--         bind_status,
--         bind_status_name,
--         is_hi_service,
--         is_privacy_enhance,
--         remind_policy_id,
--         sku,
--         quality_date,
--         customer_quality_date,
--         product_date,
--         roc_delivery_status,
--         roc_delivery_status_name,
--         is_special_asset,
--         serial_number,
--         operating_status,
--         operating_status_name,
--         running_status,
--         running_status_name,
--         environment,
--         description,
--         start_time,
--         end_time,
--         k8s_env_name,
--         create_time,
--         update_time,
--         dense_rank(partition by robot_id,rnk1-rnk2 order by end_time asc) as group_id
--     from (
--         select
--             robot_id,
--             robot_name,
--             robot_type_id,
--             robot_type_inner_name,
--             robot_type_name,
--             tenant_id,
--             rcu_id,
--             robot_account_id,
--             robot_account_name,
--             asset_code,
--             asset_type,
--             asset_type_name,
--             product_type_code,
--             product_type_code_name,
--             product_id,
--             product_id_name,
--             asset_status,
--             asset_status_name,
--             status,
--             status_name,
--             roc_status,
--             roc_status_name,
--             robot_manufacturer_id,
--             robot_manufacturer_name,
--             model,
--             os,
--             version_code,
--             software_version,
--             hardware_version,
--             head_url,
--             regist_time,
--             bind_status,
--             bind_status_name,
--             is_hi_service,
--             is_privacy_enhance,
--             remind_policy_id,
--             sku,
--             quality_date,
--             customer_quality_date,
--             product_date,
--             roc_delivery_status,
--             roc_delivery_status_name,
--             is_special_asset,
--             serial_number,
--             operating_status,
--             operating_status_name,
--             running_status,
--             running_status_name,
--             environment,
--             description,
--             start_time,
--             end_time,
--             k8s_env_name,
--             create_time,
--             update_time,
--             row_number() over(partition by robot_id order by end_time asc) as rnk1,
--             row_number() over(partition by robot_id,case when roc_status = -1 or roc_status = 2 then '删除' else '正常' end order by end_time asc) as rnk2
--         from merge_data
--     )
-- ),
-- group_column_dim as (
--     select
--         robot_id,
--         group_id,
--         collect_set(case when robot_name is not null and robot_name != '' then concat_ws('->',robot_id,end_time,robot_name) else null end ) as robot_name_dim,
--         collect_set(case when robot_type_id is not null and robot_type_id != '' then concat_ws('->',robot_id,end_time,concat_ws('###',robot_type_id,robot_type_inner_name,robot_type_name)) else null end ) as robot_type_dim,
--         collect_set(case when tenant_id is not null and tenant_id != '' then concat_ws('->',robot_id,end_time,tenant_id) else null end ) as tenant_dim,
--         collect_set(case when rcu_id is not null and rcu_id != '' then concat_ws('->',robot_id,end_time,rcu_id) else null end ) as rcu_dim,
--         collect_set(case when robot_account_id is not null and robot_account_id != '' then concat_ws('->',robot_id,end_time,concat_ws('###',robot_account_id,robot_account_name)) else null end ) as robot_account_dim,
--         collect_set(case when asset_code is not null and asset_code != '' then concat_ws('->',robot_id,end_time,asset_code) else null end ) as asset_code_dim,
--         collect_set(case when asset_type is not null and asset_type != '' then concat_ws('->',robot_id,end_time,asset_type) else null end ) as asset_type_dim,
--         collect_set(case when product_type_code is not null and product_type_code != '' then concat_ws('->',robot_id,end_time,concat_ws('###',product_type_code,product_type_code_name)) else null end ) as product_type_dim,
--         collect_set(case when (product_id is not null and product_id != '') or (product_id_name is not null and product_id_name != '') then concat_ws('->',robot_id,end_time,concat_ws('###',product_id,product_id_name)) else null end ) as product_id_dim,
--         collect_set(case when asset_status is not null and trim(cast(asset_status as string)) != '' and asset_status != -99999998 then concat_ws('->',robot_id,end_time,cast(asset_status as string)) else null end ) as asset_status_dim,
--         collect_set(case when status is not null and trim(cast(status as string)) != '' and status != -99999998 then concat_ws('->',robot_id,end_time,cast(status as string)) else null end ) as status_dim,
--         collect_set(case when roc_status is not null and trim(cast(roc_status as string)) != '' and roc_status != -99999998 then concat_ws('->',robot_id,end_time,cast(roc_status as string)) else null end ) as roc_status_dim,
--         collect_set(case when robot_manufacturer_id is not null and robot_manufacturer_id != '' then concat_ws('->',robot_id,end_time,concat_ws('###',robot_manufacturer_id,robot_manufacturer_name)) else null end ) as robot_manufacturer_dim,
--         collect_set(case when model is not null and model != '' then concat_ws('->',robot_id,end_time,model) else null end ) as model_dim,
--         collect_set(case when os is not null and os != '' then concat_ws('->',robot_id,end_time,os) else null end ) as os_dim,
--         collect_set(case when version_code is not null and version_code != '' then concat_ws('->',robot_id,end_time,version_code) else null end ) as version_code_dim,
--         collect_set(case when software_version is not null and software_version != '' then concat_ws('->',robot_id,end_time,software_version) else null end ) as software_version_dim,
--         collect_set(case when hardware_version is not null and hardware_version != '' then concat_ws('->',robot_id,end_time,hardware_version) else null end ) as hardware_version_dim,
--         collect_set(case when head_url is not null and head_url != '' then concat_ws('->',robot_id,end_time,head_url) else null end ) as head_url_dim,
--         collect_set(case when regist_time is not null and regist_time != '' then concat_ws('->',robot_id,end_time,regist_time) else null end ) as regist_time_dim,
--         collect_set(case when bind_status is not null and trim(cast(bind_status as string)) != '' and bind_status != -99999998 then concat_ws('->',robot_id,end_time,cast(bind_status as string)) else null end ) as bind_status_dim,
--         collect_set(case when is_hi_service is not null and trim(cast(is_hi_service as string)) != '' and is_hi_service != -99999998 then concat_ws('->',robot_id,end_time,cast(is_hi_service as string)) else null end ) as is_hi_service_dim,
--         collect_set(case when is_privacy_enhance is not null and trim(cast(is_privacy_enhance as string)) != '' and is_privacy_enhance != -99999998 then concat_ws('->',robot_id,end_time,cast(is_privacy_enhance as string)) else null end ) as is_privacy_enhance_dim,
--         collect_set(case when remind_policy_id is not null and remind_policy_id != '' then concat_ws('->',robot_id,end_time,remind_policy_id) else null end ) as remind_policy_id_dim,
--         collect_set(case when sku is not null and sku != '' then concat_ws('->',robot_id,end_time,sku) else null end ) as sku_dim,
--         collect_set(case when quality_date is not null and quality_date != '' then concat_ws('->',robot_id,end_time,quality_date) else null end ) as quality_date_dim,
--         collect_set(case when customer_quality_date is not null and customer_quality_date != '' then concat_ws('->',robot_id,end_time,customer_quality_date) else null end ) as customer_quality_date_dim,
--         collect_set(case when product_date is not null and product_date != '' then concat_ws('->',robot_id,end_time,product_date) else null end ) as product_date_dim,
--         collect_set(case when roc_delivery_status is not null and trim(cast(roc_delivery_status as string)) != '' and roc_delivery_status != -99999998 then concat_ws('->',robot_id,end_time,cast(roc_delivery_status as string)) else null end ) as roc_delivery_status_dim,
--         collect_set(case when is_special_asset is not null and trim(cast(is_special_asset as string)) != '' and is_special_asset != -99999998 then concat_ws('->',robot_id,end_time,cast(is_special_asset as string)) else null end ) as is_special_asset_dim,
--         collect_set(case when serial_number is not null and serial_number != '' then concat_ws('->',robot_id,end_time,serial_number) else null end ) as serial_number_dim,
--         collect_set(case when operating_status is not null and trim(cast(operating_status as string)) != '' and operating_status != -99999998 then concat_ws('->',robot_id,end_time,cast(operating_status as string)) else null end ) as operating_status_dim,
--         collect_set(case when running_status is not null and trim(cast(running_status as string)) != '' and running_status != -99999998 then concat_ws('->',robot_id,end_time,cast(running_status as string)) else null end ) as running_status_dim,
--         collect_set(case when environment is not null and environment != '' then concat_ws('->',robot_id,end_time,environment) else null end ) as environment_dim,
--         collect_set(case when description is not null and description != '' then concat_ws('->',robot_id,end_time,description) else null end ) as description_dim
--     from group_by_roc_status
--     group by robot_id, group_id
-- ),
-- 优化join
-- 补充维度，按照end_time,环境升序排序,对于空缺的属性，补充上当前robot_id,之前最近记录不为空的属性值
column_dim as (
    select
        robot_id,
        collect_set(case when robot_name is not null and robot_name != '' then concat_ws('->',robot_id,end_time,robot_name) else null end ) as robot_name_dim,
        collect_set(case when robot_type_id is not null and robot_type_id != '' then concat_ws('->',robot_id,end_time,concat_ws('###',robot_type_id,robot_type_inner_name,robot_type_name)) else null end ) as robot_type_dim,
        collect_set(case when tenant_id is not null and tenant_id != '' then concat_ws('->',robot_id,end_time,tenant_id) else null end ) as tenant_dim,
        collect_set(case when rcu_id is not null and rcu_id != '' then concat_ws('->',robot_id,end_time,rcu_id) else null end ) as rcu_dim,
        collect_set(case when robot_account_id is not null and robot_account_id != '' then concat_ws('->',robot_id,end_time,concat_ws('###',robot_account_id,robot_account_name)) else null end ) as robot_account_dim,
        collect_set(case when asset_code is not null and asset_code != '' then concat_ws('->',robot_id,end_time,asset_code) else null end ) as asset_code_dim,
        collect_set(case when asset_type is not null and asset_type != '' then concat_ws('->',robot_id,end_time,asset_type) else null end ) as asset_type_dim,
        collect_set(case when product_type_code is not null and product_type_code != '' then concat_ws('->',robot_id,end_time,concat_ws('###',product_type_code,product_type_code_name)) else null end ) as product_type_dim,
        collect_set(case when (product_id is not null and product_id != '') or (product_id_name is not null and product_id_name != '') then concat_ws('->',robot_id,end_time,concat_ws('###',product_id,product_id_name)) else null end ) as product_id_dim,
        collect_set(case when asset_status is not null and trim(cast(asset_status as string)) != '' and asset_status != -99999998 then concat_ws('->',robot_id,end_time,cast(asset_status as string)) else null end ) as asset_status_dim,
        collect_set(case when status is not null and trim(cast(status as string)) != '' and status != -99999998 then concat_ws('->',robot_id,end_time,cast(status as string)) else null end ) as status_dim,
        collect_set(case when roc_status is not null and trim(cast(roc_status as string)) != '' and roc_status != -99999998 then concat_ws('->',robot_id,end_time,cast(roc_status as string)) else null end ) as roc_status_dim,
        collect_set(case when robot_manufacturer_id is not null and robot_manufacturer_id != '' then concat_ws('->',robot_id,end_time,concat_ws('###',robot_manufacturer_id,robot_manufacturer_name)) else null end ) as robot_manufacturer_dim,
        collect_set(case when model is not null and model != '' then concat_ws('->',robot_id,end_time,model) else null end ) as model_dim,
        collect_set(case when os is not null and os != '' then concat_ws('->',robot_id,end_time,os) else null end ) as os_dim,
        collect_set(case when version_code is not null and version_code != '' then concat_ws('->',robot_id,end_time,version_code) else null end ) as version_code_dim,
        collect_set(case when software_version is not null and software_version != '' then concat_ws('->',robot_id,end_time,software_version) else null end ) as software_version_dim,
        collect_set(case when hardware_version is not null and hardware_version != '' then concat_ws('->',robot_id,end_time,hardware_version) else null end ) as hardware_version_dim,
        collect_set(case when head_url is not null and head_url != '' then concat_ws('->',robot_id,end_time,head_url) else null end ) as head_url_dim,
        collect_set(case when regist_time is not null and regist_time != '' then concat_ws('->',robot_id,end_time,regist_time) else null end ) as regist_time_dim,
        collect_set(case when bind_status is not null and trim(cast(bind_status as string)) != '' and bind_status != -99999998 then concat_ws('->',robot_id,end_time,cast(bind_status as string)) else null end ) as bind_status_dim,
        collect_set(case when is_hi_service is not null and trim(cast(is_hi_service as string)) != '' and is_hi_service != -99999998 then concat_ws('->',robot_id,end_time,cast(is_hi_service as string)) else null end ) as is_hi_service_dim,
        collect_set(case when is_privacy_enhance is not null and trim(cast(is_privacy_enhance as string)) != '' and is_privacy_enhance != -99999998 then concat_ws('->',robot_id,end_time,cast(is_privacy_enhance as string)) else null end ) as is_privacy_enhance_dim,
        collect_set(case when remind_policy_id is not null and remind_policy_id != '' then concat_ws('->',robot_id,end_time,remind_policy_id) else null end ) as remind_policy_id_dim,
        collect_set(case when sku is not null and sku != '' then concat_ws('->',robot_id,end_time,sku) else null end ) as sku_dim,
        collect_set(case when quality_date is not null and quality_date != '' then concat_ws('->',robot_id,end_time,quality_date) else null end ) as quality_date_dim,
        collect_set(case when customer_quality_date is not null and customer_quality_date != '' then concat_ws('->',robot_id,end_time,customer_quality_date) else null end ) as customer_quality_date_dim,
        collect_set(case when product_date is not null and product_date != '' then concat_ws('->',robot_id,end_time,product_date) else null end ) as product_date_dim,
        collect_set(case when roc_delivery_status is not null and trim(cast(roc_delivery_status as string)) != '' and roc_delivery_status != -99999998 then concat_ws('->',robot_id,end_time,cast(roc_delivery_status as string)) else null end ) as roc_delivery_status_dim,
        collect_set(case when is_special_asset is not null and trim(cast(is_special_asset as string)) != '' and is_special_asset != -99999998 then concat_ws('->',robot_id,end_time,cast(is_special_asset as string)) else null end ) as is_special_asset_dim,
        collect_set(case when serial_number is not null and serial_number != '' then concat_ws('->',robot_id,end_time,serial_number) else null end ) as serial_number_dim,
        collect_set(case when operating_status is not null and trim(cast(operating_status as string)) != '' and operating_status != -99999998 then concat_ws('->',robot_id,end_time,cast(operating_status as string)) else null end ) as operating_status_dim,
        collect_set(case when running_status is not null and trim(cast(running_status as string)) != '' and running_status != -99999998 then concat_ws('->',robot_id,end_time,cast(running_status as string)) else null end ) as running_status_dim,
        collect_set(case when environment is not null and environment != '' then concat_ws('->',robot_id,end_time,environment) else null end ) as environment_dim,
        collect_set(case when description is not null and description != '' then concat_ws('->',robot_id,end_time,description) else null end ) as description_dim
    from merge_data
    group by robot_id
),
-- roc_dim as (
--     select
--         t1.robot_id,
--         t1.robot_name,
--         t1.robot_type_id,
--         t1.robot_type_inner_name,
--         t1.robot_type_name,
--         t1.tenant_id,
--         t1.rcu_id,
--         t1.robot_account_id,
--         t1.robot_account_name,
--         t1.asset_code,
--         t1.asset_type,
--         t1.asset_type_name,
--         t1.product_type_code,
--         t1.product_type_code_name,
--         t1.product_id,
--         t1.product_id_name,
--         t1.asset_status,
--         t1.asset_status_name,
--         t1.status,
--         t1.status_name,
--         t1.roc_status,
--         t1.roc_status_name,
--         t1.robot_manufacturer_id,
--         t1.robot_manufacturer_name,
--         t1.model,
--         t1.os,
--         t1.version_code,
--         t1.software_version,
--         t1.hardware_version,
--         t1.head_url,
--         t1.regist_time,
--         t1.bind_status,
--         t1.bind_status_name,
--         t1.is_hi_service,
--         t1.is_privacy_enhance,
--         t1.remind_policy_id,
--         t1.sku,
--         t1.quality_date,
--         t1.customer_quality_date,
--         t1.product_date,
--         t1.roc_delivery_status,
--         t1.roc_delivery_status_name,
--         t1.is_special_asset,
--         t1.serial_number,
--         t1.operating_status,
--         t1.operating_status_name,
--         t1.running_status,
--         t1.running_status_name,
--         t1.environment,
--         t1.description,
--         t1.start_time,
--         t1.end_time,
--         t1.k8s_env_name,
--         t1.create_time,
--         t1.update_time
--     from group_by_roc_status t1
--     left join group_column_dim t2 on t1.robot_id = t2.robot_id and t1.group_id = t2.group_id
-- )
dim_repair as (
    select
        x1.robot_id,
        case when x1.robot_name = '' and nvl(x1.robot_name_dim,'') != '' then x1.robot_name_dim
             else x1.robot_name
        end as robot_name,
        case when x1.robot_type_id = '' and nvl(x1.robot_type_dim,'') != '' then split(x1.robot_type_dim,'###')[0]
             else x1.robot_type_id
        end as robot_type_id,
        case when x1.robot_type_inner_name = '' and nvl(x1.robot_type_dim,'') != '' then split(x1.robot_type_dim,'###')[1]
             else x1.robot_type_inner_name
        end as robot_type_inner_name,
        case when x1.robot_type_name = '' and nvl(x1.robot_type_dim,'') != '' then split(x1.robot_type_dim,'###')[2]
             else x1.robot_type_name
        end as robot_type_name,
        case when x1.tenant_id != '' then x1.tenant_id
             else nvl(x1.tenant_dim,'')
        end as tenant_id,
        case when x1.rcu_id != '' then x1.rcu_id
             else nvl(x1.rcu_dim,'')
        end as rcu_id,
        case when x1.robot_account_id != '' then x1.robot_account_id
             else nvl(split(x1.robot_account_dim,'###')[0],'')
        end as robot_account_id,
        case when x1.robot_account_name != '' then x1.robot_account_name
             else nvl(split(x1.robot_account_dim,'###')[1],'')
        end as robot_account_name,
        case when x1.asset_code = '' and nvl(x1.asset_code_dim,'') != '' then x1.asset_code_dim
             else x1.asset_code
        end as asset_code,
        case when x1.asset_type = '' and nvl(x1.asset_type_dim,'') != '' then x1.asset_type_dim
             else x1.asset_type
        end as asset_type,
        case when x1.product_type_code = '' and nvl(x1.product_type_dim,'') != '' then split(x1.product_type_dim,'###')[0]
             else x1.product_type_code
        end as product_type_code,
        case when x1.product_type_code_name = '' and nvl(x1.product_type_dim,'') != '' then split(x1.product_type_dim,'###')[1]
             else x1.product_type_code_name
        end as product_type_code_name,
        case when x1.product_id = '' and nvl(x1.product_id_dim,'') != '' then split(x1.product_id_dim,'###')[0]
             else x1.product_id
        end as product_id,
        case when x1.product_id_name = '' and nvl(x1.product_id_dim,'') != '' then split(x1.product_id_dim,'###')[1]
             else x1.product_id_name
        end as product_id_name,
        case when nvl(x1.asset_status_dim,'') != '' and cast(x1.asset_status_dim as int) != -99999998 then cast(x1.asset_status_dim as int)
             else nvl(x1.asset_status,-99999998)
        end as asset_status,
        case when nvl(x1.status_dim,'') != '' and cast(x1.status_dim as int) != -99999998 then cast(x1.status_dim as int)
             else nvl(x1.status,-99999998)
        end as status,
        case when x1.roc_status = -99999998 and nvl(x1.roc_status_dim,'') != '' and cast(x1.roc_status_dim as int) != -99999998 then cast(x1.roc_status_dim as int)
             else nvl(x1.roc_status,-99999998)
        end as roc_status,
        case when x1.robot_manufacturer_id = '' and nvl(x1.robot_manufacturer_dim,'') != '' then split(x1.robot_manufacturer_dim,'###')[0]
             else x1.robot_manufacturer_id
        end as robot_manufacturer_id,
        case when x1.robot_manufacturer_name = '' and nvl(x1.robot_manufacturer_dim,'') != '' then split(x1.robot_manufacturer_dim,'###')[1]
             else x1.robot_manufacturer_name
        end as robot_manufacturer_name,
        case when x1.model = '' and nvl(x1.model_dim,'') != '' then x1.model_dim
             else x1.model
        end as model,
        case when x1.os = '' and nvl(x1.os_dim,'') != '' then x1.os_dim
             else x1.os
        end as os,
        case when x1.version_code = '' and nvl(x1.version_code_dim,'') != '' then x1.version_code_dim
             else x1.version_code
        end as version_code,
        case when x1.software_version = '' and nvl(x1.software_version_dim,'') != '' then x1.software_version_dim
             else x1.software_version
        end as software_version,
        case when x1.hardware_version = '' and nvl(x1.hardware_version_dim,'') != '' then x1.hardware_version_dim
             else x1.hardware_version
        end as hardware_version,
        case when x1.head_url = '' and nvl(x1.head_url_dim,'') != '' then x1.head_url_dim
             else x1.head_url
        end as head_url,
        case when x1.regist_time = '' and nvl(x1.regist_time_dim,'') != '' then x1.regist_time_dim
             else x1.regist_time
        end as regist_time,
        case when x1.bind_status = -99999998 and nvl(x1.bind_status_dim,'') != '' and cast(x1.bind_status_dim as int) != -99999998 then cast(x1.bind_status_dim as int)
             else nvl(x1.bind_status,-99999998)
        end as bind_status,
        case when x1.is_hi_service = -99999998 and nvl(x1.is_hi_service_dim,'') != '' and cast(x1.is_hi_service_dim as int) != -99999998 then cast(x1.is_hi_service_dim as int)
             else nvl(x1.is_hi_service,-99999998)
        end as is_hi_service,
        case when x1.is_privacy_enhance = -99999998 and nvl(x1.is_privacy_enhance_dim,'') != '' and cast(x1.is_privacy_enhance_dim as int) != -99999998 then cast(x1.is_privacy_enhance_dim as int)
             else nvl(x1.is_privacy_enhance,-99999998)
        end as is_privacy_enhance,
        case when x1.remind_policy_id = -99999998 and nvl(x1.remind_policy_id_dim,'') != '' then x1.remind_policy_id_dim
             else x1.remind_policy_id
        end as remind_policy_id,
        case when x1.sku = '' and nvl(x1.sku_dim,'') != '' then x1.sku_dim
             else x1.sku
        end as sku,
        case when x1.quality_date = '' and nvl(x1.quality_date_dim,'') != '' then x1.quality_date_dim
             else x1.quality_date
        end as quality_date,
        case when x1.customer_quality_date = '' and nvl(x1.customer_quality_date_dim,'') != '' then x1.customer_quality_date_dim
             else x1.customer_quality_date
        end as customer_quality_date,
        case when x1.product_date = '' and nvl(x1.product_date_dim,'') != '' then x1.product_date_dim
             else x1.product_date
        end as product_date,
        case when nvl(x1.roc_delivery_status_dim,'') != '' and cast(x1.roc_delivery_status_dim as int) != -99999998 then cast(x1.roc_delivery_status_dim as int)
             else nvl(x1.roc_delivery_status,-99999998)
        end as roc_delivery_status,
        case when x1.is_special_asset = -99999998 and nvl(x1.is_special_asset_dim,'') != '' and cast(x1.is_special_asset_dim as int) != -99999998 then cast(x1.is_special_asset_dim as int)
             else nvl(x1.is_special_asset,-99999998)
        end as is_special_asset,
        case when nvl(x1.serial_number_dim,'') != '' then x1.serial_number_dim
             else x1.serial_number
        end as serial_number,
        case when nvl(x1.operating_status_dim,'') != '' and cast(x1.operating_status_dim as int) != -99999998 then cast(x1.operating_status_dim as int)
             else nvl(x1.operating_status,-99999998)
        end as operating_status,
        case when nvl(x1.running_status_dim,'') != '' and cast(x1.running_status_dim as int) != -99999998 then cast(x1.running_status_dim as int)
             else nvl(x1.running_status,-99999998)
        end as running_status,
        case when nvl(x1.environment_dim,'') != '' then x1.environment_dim
             else x1.environment
        end as environment,
        case when nvl(x1.description_dim,'') != '' then split(x1.description_dim,'###')[0]
             else x1.description
        end as description,
        x1.start_time,
        x1.end_time,
        x1.k8s_env_name,
        x1.create_time,
        x1.update_time
    from (
        select
            t1.robot_id,
            t1.robot_name,
            t1.robot_type_id,
            t1.robot_type_inner_name,
            t1.robot_type_name,
            t1.tenant_id,
            t1.rcu_id,
            t1.robot_account_id,
            t1.robot_account_name,
            t1.asset_code,
            t1.asset_type,
            t1.asset_type_name,
            t1.product_type_code,
            t1.product_type_code_name,
            t1.product_id,
            t1.product_id_name,
            t1.asset_status,
            t1.asset_status_name,
            t1.status,
            t1.status_name,
            t1.roc_status,
            t1.roc_status_name,
            t1.robot_manufacturer_id,
            t1.robot_manufacturer_name,
            t1.model,
            t1.os,
            t1.version_code,
            t1.software_version,
            t1.hardware_version,
            t1.head_url,
            t1.regist_time,
            t1.bind_status,
            t1.bind_status_name,
            t1.is_hi_service,
            t1.is_privacy_enhance,
            t1.remind_policy_id,
            t1.sku,
            t1.quality_date,
            t1.customer_quality_date,
            t1.product_date,
            t1.roc_delivery_status,
            t1.roc_delivery_status_name,
            t1.is_special_asset,
            t1.serial_number,
            t1.operating_status,
            t1.operating_status_name,
            t1.running_status,
            t1.running_status_name,
            t1.environment,
            t1.description,
            t1.start_time,
            t1.end_time,
            t1.k8s_env_name,
            case when t1.tmp_create_time is not null and t1.tmp_create_time != '' then t1.tmp_create_time
                 else t1.create_time
            end as create_time,
            t1.update_time,
            case when t2.robot_id is not null and size(t2.robot_name_dim) > 0 then cdmudf.dim(t1.end_time,t2.robot_name_dim)
                 else ''
            end as robot_name_dim,
            case when t2.robot_id is not null and size(t2.robot_type_dim) > 0 then cdmudf.dim(t1.end_time,t2.robot_type_dim)
                 else ''
            end as robot_type_dim,
            case when t2.robot_id is not null and size(t2.robot_type_dim) > 0 then cdmudf.dim(t1.end_time,t2.tenant_dim)
                 else ''
            end as tenant_dim,
            case when t2.robot_id is not null and size(t2.rcu_dim) > 0 then cdmudf.dim(t1.end_time,t2.rcu_dim)
                 else ''
            end as rcu_dim,
            case when t2.robot_id is not null and size(t2.robot_account_dim) > 0 then cdmudf.dim(t1.end_time,t2.robot_account_dim)
                 else ''
            end as robot_account_dim,
            case when t2.robot_id is not null and size(t2.asset_code_dim) > 0 then cdmudf.dim(t1.end_time,t2.asset_code_dim)
                 else ''
            end as asset_code_dim,
            case when t2.robot_id is not null and size(t2.asset_type_dim) > 0 then cdmudf.dim(t1.end_time,t2.asset_type_dim)
                 else ''
            end as asset_type_dim,
            case when t2.robot_id is not null and size(t2.product_type_dim) > 0 then cdmudf.dim(t1.end_time,t2.product_type_dim)
                 else ''
            end as product_type_dim,
            case when t2.robot_id is not null and size(t2.product_id_dim) > 0 then cdmudf.dim(t1.end_time,t2.product_id_dim)
                 else ''
            end as product_id_dim,
            case when t2.robot_id is not null and size(t2.asset_status_dim) > 0 then cdmudf.dim(t1.end_time,t2.asset_status_dim)
                 else ''
            end as asset_status_dim,
            case when t2.robot_id is not null and size(t2.status_dim) > 0 then cdmudf.dim(t1.end_time,t2.status_dim)
                 else ''
            end as status_dim,
            case when t2.robot_id is not null and size(t2.roc_status_dim) > 0 then cdmudf.dim(t1.end_time,t2.roc_status_dim)
                 else ''
            end as roc_status_dim,
            case when t2.robot_id is not null and size(t2.robot_manufacturer_dim) > 0 then cdmudf.dim(t1.end_time,t2.robot_manufacturer_dim)
                 else ''
            end as robot_manufacturer_dim,
            case when t2.robot_id is not null and size(t2.model_dim) > 0 then cdmudf.dim(t1.end_time,t2.model_dim)
                 else ''
            end as model_dim,
            case when t2.robot_id is not null and size(t2.os_dim) > 0 then cdmudf.dim(t1.end_time,t2.os_dim)
                 else ''
            end as os_dim,
            case when t2.robot_id is not null and size(t2.version_code_dim) > 0 then cdmudf.dim(t1.end_time,t2.version_code_dim)
                 else ''
            end as version_code_dim,
            case when t2.robot_id is not null and size(t2.software_version_dim) > 0 then cdmudf.dim(t1.end_time,t2.software_version_dim)
                 else ''
            end as software_version_dim,
            case when t2.robot_id is not null and size(t2.hardware_version_dim) > 0 then cdmudf.dim(t1.end_time,t2.hardware_version_dim)
                 else ''
            end as hardware_version_dim,
            case when t2.robot_id is not null and size(t2.head_url_dim) > 0 then cdmudf.dim(t1.end_time,t2.head_url_dim)
                 else ''
            end as head_url_dim,
            case when t2.robot_id is not null and size(t2.regist_time_dim) > 0 then cdmudf.dim(t1.end_time,t2.regist_time_dim)
                 else ''
            end as regist_time_dim,
            case when t2.robot_id is not null and size(t2.bind_status_dim) > 0 then cdmudf.dim(t1.end_time,t2.bind_status_dim)
                 else ''
            end as bind_status_dim,
            case when t2.robot_id is not null and size(t2.is_hi_service_dim) > 0 then cdmudf.dim(t1.end_time,t2.is_hi_service_dim)
                 else ''
            end as is_hi_service_dim,
            case when t2.robot_id is not null and size(t2.is_privacy_enhance_dim) > 0 then cdmudf.dim(t1.end_time,t2.is_privacy_enhance_dim)
                 else ''
            end as is_privacy_enhance_dim,
            case when t2.robot_id is not null and size(t2.remind_policy_id_dim) > 0 then cdmudf.dim(t1.end_time,t2.remind_policy_id_dim)
                 else ''
            end as remind_policy_id_dim,
            case when t2.robot_id is not null and size(t2.sku_dim) > 0 then cdmudf.dim(t1.end_time,t2.sku_dim)
                 else ''
            end as sku_dim,
            case when t2.robot_id is not null and size(t2.quality_date_dim) > 0 then cdmudf.dim(t1.end_time,t2.quality_date_dim)
                 else ''
            end as quality_date_dim,
            case when t2.robot_id is not null and size(t2.customer_quality_date_dim) > 0 then cdmudf.dim(t1.end_time,t2.customer_quality_date_dim)
                 else ''
            end as customer_quality_date_dim,
            case when t2.robot_id is not null and size(t2.product_date_dim) > 0 then cdmudf.dim(t1.end_time,t2.product_date_dim)
                 else ''
            end as product_date_dim,
            case when t2.robot_id is not null and size(t2.roc_delivery_status_dim) > 0 then cdmudf.dim(t1.end_time,t2.roc_delivery_status_dim)
                 else ''
            end as roc_delivery_status_dim,
            case when t2.robot_id is not null and size(t2.is_special_asset_dim) > 0 then cdmudf.dim(t1.end_time,t2.is_special_asset_dim)
                 else ''
            end as is_special_asset_dim,
            case when t2.robot_id is not null and size(t2.serial_number_dim) > 0 then cdmudf.dim(t1.end_time,t2.serial_number_dim)
                 else ''
            end as serial_number_dim,
            case when t2.robot_id is not null and size(t2.operating_status_dim) > 0 then cdmudf.dim(t1.end_time,t2.operating_status_dim)
                 else ''
            end as operating_status_dim,
            case when t2.robot_id is not null and size(t2.running_status_dim) > 0 then cdmudf.dim(t1.end_time,t2.running_status_dim)
                 else ''
            end as running_status_dim,
            case when t2.robot_id is not null and size(t2.environment_dim) > 0 then cdmudf.dim(t1.end_time,t2.environment_dim)
                 else ''
            end as environment_dim,
            case when t2.robot_id is not null and size(t2.description_dim) > 0 then cdmudf.dim(t1.end_time,t2.description_dim)
                 else ''
            end as description_dim
        from merge_data t1
        left join column_dim t2 on t1.robot_id = t2.robot_id
    ) x1
),
dim_add as (
    select
        t1.robot_id,
        t1.robot_name,
        t1.robot_type_id,
        t1.robot_type_inner_name,
        t1.robot_type_name,
        t1.tenant_id,
        t1.rcu_id,
        t1.robot_account_id,
        t1.robot_account_name,
        t1.asset_code,
        t1.asset_type,
         case when trim(t1.asset_type) = '1' then '达闼固资'
              when trim(t1.asset_type) = '2' then '达闼存资'
              when trim(t1.asset_type) = '3' then '客户资产'
              else '未知'
         end as asset_type_name,
        t1.product_type_code,
        t1.product_type_code_name,
        t1.product_id,
        t1.product_id_name,
        t1.asset_status,
         case when t1.asset_status = 1 then '在册'
              when t1.asset_status = 2 then '待测'
              when t1.asset_status = 3 then '研发测试'
              when t1.asset_status = 4 then '空闲'
              when t1.asset_status = 5 then '项目中'
              else '未知'
         end as asset_status_name,
        t1.status,
        case when t1.status = 1 then '正常'
             when t1.status = 9 then '删除'
             else '未知'
        end as status_name,
        t1.roc_status,
        case when roc_status = 0 then '正常'
             when roc_status = 1 then '停用'
             when roc_status = -1 then '删除'
             when roc_status = 2 then '待删除'
             else '未知'
        end as roc_status_name,
        t1.robot_manufacturer_id,
        case when t1.robot_manufacturer_name != '' then t1.robot_manufacturer_name
             else nvl(t2.robot_manufacturer_name,'')
        end as robot_manufacturer_name,
        t1.model,
        t1.os,
        t1.version_code,
        t1.software_version,
        t1.hardware_version,
        t1.head_url,
        t1.regist_time,
        t1.bind_status,
        case when bind_status = -1 then '删除'
             when bind_status = 0 then '注册未激活'
             when bind_status = 1 then '激活可用'
             when bind_status = 2 then '通知VPN注册'
             else '未知'
        end as bind_status_name,
        t1.is_hi_service,
        t1.is_privacy_enhance,
        t1.remind_policy_id,
        t1.sku,
        t1.quality_date,
        t1.customer_quality_date,
        t1.product_date,
        t1.roc_delivery_status,
        case when t1.roc_delivery_status = 0 then '已交付'
             when t1.roc_delivery_status = 1 then '未交付'
             when t1.roc_delivery_status = 2 then '交付中'
             when t1.roc_delivery_status = 3 then '回收中'
             else '未知'
        end as roc_delivery_status_name,
        t1.is_special_asset,
        t1.serial_number,
        t1.operating_status,
        case when t1.operating_status = 1 then '空闲'
             when t1.operating_status = 2 then '测试中'
             when t1.operating_status = 3 then '演示中'
             when t1.operating_status = 4 then '运营中'
             when t1.operating_status = 5 then '交付中'
             else '未知'
        end as operating_status_name,
        t1.running_status,
        case when t1.running_status = 1 then '良好'
             when t1.running_status = 2 then '故障'
             when t1.running_status = 3 then '维修中'
             else '未知'
        end as running_status_name,
        t1.environment,
        t1.description,
        t1.start_time,
        t1.end_time,
        t1.k8s_env_name,
        t1.create_time,
        t1.update_time
    from dim_repair t1
    left join crio_t_dict t2 on trim(t1.robot_manufacturer_id) = t2.robot_manufacturer_id
),
-- 使用两次排序方案，对数据去重保持状态延续
-- 先按照robot_id分组,end_time升序排序
-- 然后按照robot_id,关键属性md5分组,end_time升序排序
-- 分组后的数值做差,按照robot_id,关键属性md5,差值分组,组内更新end_time时间,返回更新后的记录即可
state_continue as (
    select
        robot_id,
        robot_name,
        robot_type_id,
        robot_type_inner_name,
        robot_type_name,
        tenant_id,
        rcu_id,
        robot_account_id,
        robot_account_name,
        asset_code,
        asset_type,
        asset_type_name,
        product_type_code,
        product_type_code_name,
        product_id,
        product_id_name,
        asset_status,
        asset_status_name,
        status,
        status_name,
        roc_status,
        roc_status_name,
        robot_manufacturer_id,
        robot_manufacturer_name,
        model,
        os,
        version_code,
        software_version,
        hardware_version,
        head_url,
        regist_time,
        bind_status,
        bind_status_name,
        is_hi_service,
        is_privacy_enhance,
        remind_policy_id,
        sku,
        quality_date,
        customer_quality_date,
        product_date,
        roc_delivery_status,
        roc_delivery_status_name,
        is_special_asset,
        serial_number,
        operating_status,
        operating_status_name,
        running_status,
        running_status_name,
        environment,
        description,
        min(start_time) as start_time,
        max(end_time) as end_time,
        k8s_env_name,
        create_time,
        min(update_time) as update_time,
        row_number() OVER (PARTITION BY robot_id ORDER BY create_time, min(update_time) asc) as rnk
    from (
        select
            robot_id,
            robot_name,
            robot_type_id,
            robot_type_inner_name,
            robot_type_name,
            tenant_id,
            rcu_id,
            robot_account_id,
            robot_account_name,
            asset_code,
            asset_type,
            asset_type_name,
            product_type_code,
            product_type_code_name,
            product_id,
            product_id_name,
            asset_status,
            asset_status_name,
            status,
            status_name,
            roc_status,
            roc_status_name,
            robot_manufacturer_id,
            robot_manufacturer_name,
            model,
            os,
            version_code,
            software_version,
            hardware_version,
            head_url,
            regist_time,
            bind_status,
            bind_status_name,
            is_hi_service,
            is_privacy_enhance,
            remind_policy_id,
            sku,
            quality_date,
            customer_quality_date,
            product_date,
            roc_delivery_status,
            roc_delivery_status_name,
            is_special_asset,
            serial_number,
            operating_status,
            operating_status_name,
            running_status,
            running_status_name,
            environment,
            description,
            start_time,
            end_time,
            k8s_env_name,
            create_time,
            update_time,
            row_number() over(partition by robot_id order by end_time asc) as rnk1,
            row_number() over(partition by robot_id,robot_name,robot_type_id,robot_type_inner_name,robot_type_name,tenant_id,rcu_id,robot_account_id,robot_account_name,asset_code,asset_type,asset_type_name,product_type_code,product_type_code_name,product_id,product_id_name,asset_status,asset_status_name,status,status_name,roc_status,roc_status_name,robot_manufacturer_id,robot_manufacturer_name,model,os,version_code,software_version,hardware_version,head_url,regist_time,bind_status,bind_status_name,is_hi_service,is_privacy_enhance,remind_policy_id,sku,quality_date,customer_quality_date,product_date,roc_delivery_status,roc_delivery_status_name,is_special_asset,serial_number,operating_status,operating_status_name,running_status,running_status_name,environment,description,k8s_env_name,create_time order by end_time asc) as rnk2
        from dim_add
    ) t group by robot_id,robot_name,robot_type_id,robot_type_inner_name,robot_type_name,tenant_id,rcu_id,robot_account_id,robot_account_name,asset_code,asset_type,asset_type_name,product_type_code,product_type_code_name,product_id,product_id_name,asset_status,asset_status_name,status,status_name,roc_status,roc_status_name,robot_manufacturer_id,robot_manufacturer_name,model,os,version_code,software_version,hardware_version,head_url,regist_time,bind_status,bind_status_name,is_hi_service,is_privacy_enhance,remind_policy_id,sku,quality_date,customer_quality_date,product_date,roc_delivery_status,roc_delivery_status_name,is_special_asset,serial_number,operating_status,operating_status_name,running_status,running_status_name,environment,description,k8s_env_name,create_time,rnk2-rnk1
),
-- 首条记录保持 使用create_time作为start_time
modify_start_time as (
    select
        robot_id,
        robot_name,
        robot_type_id,
        robot_type_inner_name,
        robot_type_name,
        tenant_id,
        asset_code,
        asset_type,
        asset_type_name,
        product_type_code,
        product_type_code_name,
        product_id,
        product_id_name,
        assets_status,
        assets_status_name,
        status,
        status_name,
        robot_manufacturer_id,
        robot_manufacturer_name,
        model,
        os,
        version_code,
        software_version,
        hardware_version,
        head_url,
        regist_time,
        bind_status,
        bind_status_name,
        is_hi_service,
        is_privacy_enhance,
        remind_policy_id,
        sku,
        quality_date,
        customer_quality_date,
        product_date,
        roc_delivery_status,
        roc_delivery_status_name,
        is_special_asset,
        serial_number,
        operating_status,
        operating_status_name,
        running_status,
        running_status_name,
        environment,
        description,
        case when rnk = 1 then create_time
             else start_time
        end as start_time,
        end_time,
        k8s_env_name,
        create_time,
        update_time
    from state_continue
)
insert overwrite table cdmdim.dim_pmd_robot_sh_d partition(dt)
    select
        robot_id,
        robot_name,
        robot_type_id,
        robot_type_inner_name,
        robot_type_name,
        tenant_id,
        asset_code,
        asset_type,
        asset_type_name,
        product_type_code,
        product_type_code_name,
        product_id,
        product_id_name,
        assets_status,
        assets_status_name,
        status,
        status_name,
        robot_manufacturer_id,
        robot_manufacturer_name,
        model,
        os,
        version_code,
        software_version,
        hardware_version,
        head_url,
        regist_time,
        bind_status,
        bind_status_name,
        is_hi_service,
        is_privacy_enhance,
        remind_policy_id,
        sku,
        quality_date,
        customer_quality_date,
        product_date,
        roc_delivery_status,
        roc_delivery_status_name,
        is_special_asset,
        serial_number,
        operating_status,
        operating_status_name,
        running_status,
        running_status_name,
        environment,
        description,
        start_time,
        end_time,
        k8s_env_name,
        create_time,
        update_time,
        '${e_dt_var}' as dt
    from modify_start_time;


-- 16号拉了device快照时间没有加8,17号binlog自动加了8手动补充
-- 获取当前表的最大快照日期，用来判断变化的数据，如果未获取到，需先执行手动脚本初始化数据
with roc_t_dict as (
    select
        t1.robot_type_id,
        t1.robot_type_name,
        t2.robot_type_inner_name
    from (
             select
                 label as robot_type_name,
                 `value` as robot_type_id
             from cdmods.ods_roc_db_c0002_t_dict_s_d
             where dt = '${e_dt_var}' and TYPE = 'robot-type'
             group by label, `value`
         ) t1
             full join (
        select
            label as robot_type_inner_name,
            `value` as robot_type_id
        from cdmods.ods_roc_db_c0002_t_dict_s_d
        where dt = '${e_dt_var}' and TYPE = 'robot-type-hari'
        group by label, `value`
    ) t2 on t1.robot_type_id = t2.robot_type_id
),
-- 机器人制造商表 type=2
     crio_t_dict as (
         select
             trim(code) as robot_manufacturer_id,
             trim(name) as robot_manufacturer_name
         from cdmods.ods_crio_db_c0001_t_dict_s_d where dt = '${e_dt_var}' and `type` = 2
     ),
     crio_t_device as (
         select
             x1.robot_id,
             x1.robot_name,
             x1.robot_type_id,
             nvl(x2.robot_type_inner_name,'') as robot_type_inner_name,
             x1.robot_type_name,
             x1.asset_code,
             x1.asset_type,
             x1.product_type_code,
             x1.product_type_code_name,
             x1.product_id,
             x1.product_id_name,
             x1.assets_status,
             x1.assets_status_name,
             x1.status,
             x1.status_name,
             nvl(x1.robot_manufacturer_id,'') as robot_manufacturer_id,
             case when nvl(x3.robot_manufacturer_name,'') != '' then x3.robot_manufacturer_name
                  else nvl(x1.robot_manufacturer_name,'')
                 end as robot_manufacturer_name,
             x1.model,
             x1.os,
             x1.version_code,
             x1.software_version,
             x1.hardware_version,
             x1.head_url,
             x1.regist_time,
             x1.bind_status,
             x1.bind_status_name,
             x1.is_hi_service,
             x1.is_privacy_enhance,
             x1.remind_policy_id,
             x1.sku,
             x1.quality_date,
             x1.customer_quality_date,
             x1.out_type_state,
             x1.out_type_state_name,
             x1.product_date,
             x1.in_stock_date,
             x1.out_stock_date,
             x1.in_stock_staff_id,
             x1.in_stock_staff_name,
             x1.out_stock_staff_id,
             x1.out_stock_staff_name,
             x1.note,
             x1.description,
             x1.create_time,
             x1.update_time,
             x1.k8s_env_name
         from (
                  select
                      nvl(t1.device_code,'') as robot_id,
                      regexp_replace(nvl(trim(t1.device_name),''), '\r|\n|\t', '') as robot_name,
                      nvl(t2.robot_type_id,'') as robot_type_id,
                      nvl(t2.robot_type_name,'') as robot_type_name,
                      nvl(t1.asset_code,'') as asset_code,
                      nvl(t1.asset_type,'') as asset_type,
                      nvl(t1.product_type_code,'') as product_type_code,
                      nvl(t1.product_type_code_name,'') as product_type_code_name,
                      nvl(t1.product_id,'') as product_id,
                      nvl(t1.product_id_name,'') as product_id_name,
                      nvl(t1.asset_status,-99999998) as assets_status,
                      case when t1.asset_status = 0 then '在册'
                           when t1.asset_status = 1 then '在库'
                           when t1.asset_status = 2 then '出库'
                           else '未知'
                          end as assets_status_name,
                      case when t1.status = 1 then 0
                           when t1.status = 9 then -1
                           else -99999998
                          end as status,
                      case when t1.status = 1 then '正常'
                           when t1.status = 9 then '删除'
                           else '未知'
                          end as status_name,
                      nvl(supplier_code,'') as robot_manufacturer_id,
                      nvl(supplier_code_name,'') as robot_manufacturer_name,
                      nvl(t1.device_model,'') as model,
                      '' as os,
                      '' as version_code,
                      nvl(t1.software_version,'') as software_version,
                      nvl(t1.hardware_version,'') as hardware_version,
                      '' as head_url,
                      '' as regist_time,
                      -99999998 as bind_status,
                      '未知' as bind_status_name,
                      -99999998 as is_hi_service,
                      -99999998 as is_privacy_enhance,
                      '' as remind_policy_id,
                      nvl(t1.sku,'') as sku,
                      case when length(t1.quality_date) = 10 then nvl(concat(from_unixtime( cast(t1.quality_date as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
                           when length(t1.quality_date) = 19 then nvl(concat(from_utc_timestamp(t1.quality_date,'PRC'),'.000'),'')
                           when length(t1.quality_date) >= 23 then nvl(from_utc_timestamp(SUBSTRING(REGEXP_REPLACE(t1.quality_date,'T',' '),0,23),'PRC'),'')
                           else nvl(concat(from_unixtime( cast(t1.quality_date/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(t1.quality_date,11,3)),'')
                          end as quality_date,
                      case when length(t1.customer_quality_date) = 10 then nvl(concat(from_unixtime( cast(t1.customer_quality_date as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
                           when length(t1.customer_quality_date) = 19 then nvl(concat(from_utc_timestamp(t1.customer_quality_date,'PRC'),'.000'),'')
                           when length(t1.customer_quality_date) >= 23 then nvl(from_utc_timestamp(SUBSTRING(REGEXP_REPLACE(t1.customer_quality_date,'T',' '),0,23),'PRC'),'')
                           else nvl(concat(from_unixtime( cast(t1.customer_quality_date/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(t1.customer_quality_date,11,3)),'')
                          end as customer_quality_date,
                      -99999998 as out_type_state,
                      '未知' as out_type_state_name,
                      case when length(t1.product_date) = 10 then nvl(concat(from_unixtime( cast(t1.product_date as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
                           when length(t1.product_date) = 19 then nvl(concat(from_utc_timestamp(t1.product_date,'PRC'),'.000'),'')
                           when length(t1.product_date) >= 23 then nvl(from_utc_timestamp(SUBSTRING(REGEXP_REPLACE(t1.product_date,'T',' '),0,23),'PRC'),'')
                           else nvl(concat(from_unixtime( cast(t1.product_date/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(t1.product_date,11,3)),'')
                          end as product_date,
                      '' as in_stock_date,
                      '' as out_stock_date,
                      '' as in_stock_staff_id,
                      '' as in_stock_staff_name,
                      '' as out_stock_staff_id,
                      '' as out_stock_staff_name,
                      '' as note,
                      '' as description,
                      case when length(t1.create_time) = 10 then nvl(concat(from_unixtime( cast(t1.create_time as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
                           when length(t1.create_time) = 19 then nvl(concat(from_utc_timestamp(t1.create_time,'PRC'),'.000'),'')
                           when length(t1.create_time) >= 23 then nvl(from_utc_timestamp(SUBSTRING(REGEXP_REPLACE(t1.create_time,'T',' '),0,23),'PRC'),'')
                           else nvl(concat(from_unixtime( cast(t1.create_time/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(t1.create_time,11,3)),'')
                          end as create_time,
                      case when length(t1.update_time) = 10 then nvl(concat(from_unixtime( cast(t1.update_time as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
                           when length(t1.update_time) = 19 then nvl(concat(from_utc_timestamp(t1.update_time,'PRC'),'.000'),'')
                           when length(t1.update_time) >= 23 then nvl(from_utc_timestamp(SUBSTRING(REGEXP_REPLACE(t1.update_time,'T',' '),0,23),'PRC'),'')
                           else nvl(concat(from_unixtime( cast(t1.update_time/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(t1.update_time,11,3)),'')
                          end as update_time,
                      'bj-prod-232' as k8s_env_name
                  from cdmods.ods_crio_db_c0001_t_device_i_d t1
                           left join (
                      select
                          robot_type_id,
                          robot_type_name
                      from roc_t_dict
                  ) t2 on trim(lower(regexp_replace(t1.product_type_code,'\\s|-',''))) = trim(lower(regexp_replace(t2.robot_type_name,'\\s|-','')))
                  where dt = '2022-01-16'
                  union
                  select
                      nvl(t1.device_code,'') as robot_id,
                      regexp_replace(nvl(trim(t1.device_name),''), '\r|\n|\t', '') as robot_name,
                      nvl(t2.robot_type_id,'') as robot_type_id,
                      nvl(t2.robot_type_name,'') as robot_type_name,
                      nvl(t1.asset_code,'') as asset_code,
                      nvl(t1.asset_type,'') as asset_type,
                      nvl(t1.product_type_code,'') as product_type_code,
                      nvl(t1.product_type_code_name,'') as product_type_code_name,
                      nvl(t1.product_id,'') as product_id,
                      nvl(t1.product_id_name,'') as product_id_name,
                      nvl(t1.asset_status,-99999998) as assets_status,
                      case when t1.asset_status = 0 then '在册'
                           when t1.asset_status = 1 then '在库'
                           when t1.asset_status = 2 then '出库'
                           else '未知'
                          end as assets_status_name,
                      case when t1.status = 1 then 0
                           when t1.status = 9 then -1
                           else -99999998
                          end as status,
                      case when t1.status = 1 then '正常'
                           when t1.status = 9 then '删除'
                           else '未知'
                          end as status_name,
                      nvl(supplier_code,'') as robot_manufacturer_id,
                      nvl(supplier_code_name,'') as robot_manufacturer_name,
                      nvl(t1.device_model,'') as model,
                      '' as os,
                      '' as version_code,
                      nvl(t1.software_version,'') as software_version,
                      nvl(t1.hardware_version,'') as hardware_version,
                      '' as head_url,
                      '' as regist_time,
                      -99999998 as bind_status,
                      '未知' as bind_status_name,
                      -99999998 as is_hi_service,
                      -99999998 as is_privacy_enhance,
                      '' as remind_policy_id,
                      nvl(t1.sku,'') as sku,
                      case when length(t1.quality_date) = 10 then nvl(concat(from_unixtime( cast(t1.quality_date as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
                          --                            when length(t1.quality_date) = 19 then nvl(concat(from_utc_timestamp(t1.quality_date,'PRC'),'.000'),'')
--                            when length(t1.quality_date) > 19 and length(t1.quality_date) < 23 then if(LENGTH(from_utc_timestamp(t1.quality_date,'PRC'))=19,nvl(concat(from_utc_timestamp(t1.quality_date,'PRC'),'.000'),''),nvl(rpad(from_utc_timestamp(t1.quality_date,'PRC'),23,'0'),''))
--                            when length(t1.quality_date) >= 23 then nvl(from_utc_timestamp(SUBSTRING(REGEXP_REPLACE(t1.quality_date,'T',' '),0,23),'PRC'),'')
                           when length(t1.quality_date) = 19 then nvl(concat(t1.quality_date,'.000'),'')
                           when length(t1.quality_date) > 19 and length(t1.quality_date) < 23 then if(LENGTH(t1.quality_date)=19,nvl(concat(t1.quality_date,'.000'),''),nvl(rpad(t1.quality_date,23,'0'),''))
                           when length(t1.quality_date) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(t1.quality_date,'T',' '),0,23),'')
                           else nvl(concat(from_unixtime( cast(t1.quality_date/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(t1.quality_date,11,3)),'')
                          end as quality_date,
                      case when length(t1.customer_quality_date) = 10 then nvl(concat(from_unixtime( cast(t1.customer_quality_date as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
                          --                            when length(t1.customer_quality_date) = 19 then nvl(concat(from_utc_timestamp(t1.customer_quality_date,'PRC'),'.000'),'')
--                            when length(t1.customer_quality_date) > 19 and length(t1.customer_quality_date) < 23 then if(LENGTH(from_utc_timestamp(t1.customer_quality_date,'PRC'))=19,nvl(concat(from_utc_timestamp(t1.customer_quality_date,'PRC'),'.000'),''),nvl(rpad(from_utc_timestamp(t1.customer_quality_date,'PRC'),23,'0'),''))
--                            when length(t1.customer_quality_date) >= 23 then nvl(from_utc_timestamp(SUBSTRING(REGEXP_REPLACE(t1.customer_quality_date,'T',' '),0,23),'PRC'),'')
                           when length(t1.customer_quality_date) = 19 then nvl(concat(t1.customer_quality_date,'.000'),'')
                           when length(t1.customer_quality_date) > 19 and length(t1.customer_quality_date) < 23 then if(LENGTH(t1.customer_quality_date)=19,nvl(concat(t1.customer_quality_date,'.000'),''),nvl(rpad(t1.customer_quality_date,23,'0'),''))
                           when length(t1.customer_quality_date) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(t1.customer_quality_date,'T',' '),0,23),'')
                           else nvl(concat(from_unixtime( cast(t1.customer_quality_date/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(t1.customer_quality_date,11,3)),'')
                          end as customer_quality_date,
                      -99999998 as out_type_state,
                      '未知' as out_type_state_name,
                      case when length(t1.product_date) = 10 then nvl(concat(from_unixtime( cast(t1.product_date as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
                          --                            when length(t1.product_date) = 19 then nvl(concat(from_utc_timestamp(t1.product_date,'PRC'),'.000'),'')
--                            when length(t1.product_date) > 19 and length(t1.product_date) < 23 then if(LENGTH(from_utc_timestamp(t1.product_date,'PRC'))=19,nvl(concat(from_utc_timestamp(t1.product_date,'PRC'),'.000'),''),nvl(rpad(from_utc_timestamp(t1.product_date,'PRC'),23,'0'),''))
--                            when length(t1.product_date) >= 23 then nvl(from_utc_timestamp(SUBSTRING(REGEXP_REPLACE(t1.product_date,'T',' '),0,23),'PRC'),'')
                           when length(t1.product_date) = 19 then nvl(concat(t1.product_date,'.000'),'')
                           when length(t1.product_date) > 19 and length(t1.product_date) < 23 then if(LENGTH(t1.product_date)=19,nvl(concat(t1.product_date,'.000'),''),nvl(rpad(t1.product_date,23,'0'),''))
                           when length(t1.product_date) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(t1.product_date,'T',' '),0,23),'')
                           else nvl(concat(from_unixtime( cast(t1.product_date/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(t1.product_date,11,3)),'')
                          end as product_date,
                      '' as in_stock_date,
                      '' as out_stock_date,
                      '' as in_stock_staff_id,
                      '' as in_stock_staff_name,
                      '' as out_stock_staff_id,
                      '' as out_stock_staff_name,
                      '' as note,
                      '' as description,
                      case when length(t1.create_time) = 10 then nvl(concat(from_unixtime( cast(t1.create_time as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
                          --                            when length(t1.create_time) = 19 then nvl(concat(from_utc_timestamp(t1.create_time,'PRC'),'.000'),'')
--                            when length(t1.create_time) > 19 and length(t1.create_time) < 23 then if(LENGTH(from_utc_timestamp(t1.create_time,'PRC'))=19,nvl(concat(from_utc_timestamp(t1.create_time,'PRC'),'.000'),''),nvl(rpad(from_utc_timestamp(t1.create_time,'PRC'),23,'0'),''))
--                            when length(t1.create_time) >= 23 then nvl(from_utc_timestamp(SUBSTRING(REGEXP_REPLACE(t1.create_time,'T',' '),0,23),'PRC'),'')
                           when length(t1.create_time) = 19 then nvl(concat(t1.create_time,'.000'),'')
                           when length(t1.create_time) > 19 and length(t1.create_time) < 23 then if(LENGTH(t1.create_time)=19,nvl(concat(t1.create_time,'.000'),''),nvl(rpad(t1.create_time,23,'0'),''))
                           when length(t1.create_time) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(t1.create_time,'T',' '),0,23),'')
                           else nvl(concat(from_unixtime( cast(t1.create_time/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(t1.create_time,11,3)),'')
                          end as create_time,
                      case when length(t1.update_time) = 10 then nvl(concat(from_unixtime( cast(t1.update_time as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
                          --                            when length(t1.update_time) = 19 then nvl(concat(from_utc_timestamp(t1.update_time,'PRC'),'.000'),'')
--                            when length(t1.update_time) > 19 and length(t1.update_time) < 23 then if(LENGTH(from_utc_timestamp(t1.update_time,'PRC'))=19,nvl(concat(from_utc_timestamp(t1.update_time,'PRC'),'.000'),''),nvl(rpad(from_utc_timestamp(t1.update_time,'PRC'),23,'0'),''))
--                            when length(t1.update_time) >= 23 then nvl(from_utc_timestamp(SUBSTRING(REGEXP_REPLACE(t1.update_time,'T',' '),0,23),'PRC'),'')
                           when length(t1.update_time) = 19 then nvl(concat(t1.update_time,'.000'),'')
                           when length(t1.update_time) > 19 and length(t1.update_time) < 23 then if(LENGTH(t1.update_time)=19,nvl(concat(t1.update_time,'.000'),''),nvl(rpad(t1.update_time,23,'0'),''))
                           when length(t1.update_time) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(t1.update_time,'T',' '),0,23),'')
                           else nvl(concat(from_unixtime( cast(t1.update_time/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(t1.update_time,11,3)),'')
                          end as update_time,
                      'bj-prod-232' as k8s_env_name
                  from cdmods.ods_crio_db_c0001_t_device_i_d t1
                           left join (
                      select
                          robot_type_id,
                          robot_type_name
                      from roc_t_dict
                  ) t2 on trim(lower(regexp_replace(t1.product_type_code,'\\s|-',''))) = trim(lower(regexp_replace(t2.robot_type_name,'\\s|-','')))
                  where dt = '2022-01-17'
              ) x1 left join (
             select
                 robot_type_id,
                 robot_type_inner_name
             from roc_t_dict
         ) x2 on x1.robot_type_id = x2.robot_type_id
                   left join crio_t_dict x3 on nvl(trim(x1.robot_manufacturer_id),'') = x3.robot_manufacturer_id
     ),
     inc_data as (
         select
             robot_id,
             robot_name,
             robot_type_id,
             robot_type_inner_name,
             robot_type_name,
             asset_code,
             asset_type,
             product_type_code,
             product_type_code_name,
             product_id,
             product_id_name,
             assets_status,
             assets_status_name,
             status,
             status_name,
             robot_manufacturer_id,
             robot_manufacturer_name,
             model,
             os,
             version_code,
             software_version,
             hardware_version,
             head_url,
             regist_time,
             bind_status,
             bind_status_name,
             is_hi_service,
             is_privacy_enhance,
             remind_policy_id,
             sku,
             quality_date,
             customer_quality_date,
             out_type_state,
             out_type_state_name,
             product_date,
             in_stock_date,
             out_stock_date,
             in_stock_staff_id,
             in_stock_staff_name,
             out_stock_staff_id,
             out_stock_staff_name,
             note,
             description,
             create_time as start_time,
             '9999-12-31 23:59:59.999' as end_time,
             k8s_env_name,
             create_time,
             update_time
         from (
                  select
                      t1.robot_id,
                      robot_name,
                      robot_type_id,
                      robot_type_inner_name,
                      robot_type_name,
                      asset_code,
                      asset_type,
                      product_type_code,
                      product_type_code_name,
                      product_id,
                      product_id_name,
                      assets_status,
                      assets_status_name,
                      status,
                      status_name,
                      robot_manufacturer_id,
                      robot_manufacturer_name,
                      model,
                      os,
                      version_code,
                      software_version,
                      hardware_version,
                      head_url,
                      regist_time,
                      bind_status,
                      bind_status_name,
                      is_hi_service,
                      is_privacy_enhance,
                      remind_policy_id,
                      sku,
                      quality_date,
                      customer_quality_date,
                      out_type_state,
                      out_type_state_name,
                      product_date,
                      in_stock_date,
                      out_stock_date,
                      in_stock_staff_id,
                      in_stock_staff_name,
                      out_stock_staff_id,
                      out_stock_staff_name,
                      note,
                      description,
                      create_time,
                      update_time,
                      k8s_env_name,
                      row_number() OVER (PARTITION BY t1.robot_id ORDER BY create_time asc) as rnk
                  from crio_t_device t1
                           left join (
                      select
                          robot_id
                      from cdmdim.dim_pmd_robot_sh_d where dt = date_sub(current_date,1) group by robot_id
                  ) t2 on t1.robot_id = t2.robot_id
                  where t1.robot_id is not null and t2.robot_id is null
              ) x1 where x1.rnk = 1
     ),
     union_robot as (
         select
             robot_id,
             robot_name,
             robot_type_id,
             robot_type_inner_name,
             robot_type_name,
             asset_code,
             asset_type,
             product_type_code,
             product_type_code_name,
             product_id,
             product_id_name,
             assets_status,
             assets_status_name,
             status,
             status_name,
             robot_manufacturer_id,
             robot_manufacturer_name,
             model,
             os,
             version_code,
             software_version,
             hardware_version,
             head_url,
             regist_time,
             bind_status,
             bind_status_name,
             is_hi_service,
             is_privacy_enhance,
             remind_policy_id,
             sku,
             quality_date,
             customer_quality_date,
             out_type_state,
             out_type_state_name,
             product_date,
             in_stock_date,
             out_stock_date,
             in_stock_staff_id,
             in_stock_staff_name,
             out_stock_staff_id,
             out_stock_staff_name,
             note,
             description,
             start_time,
             end_time,
             k8s_env_name,
             create_time,
             update_time
         from cdmdim.dim_pmd_robot_sh_d where dt = date_sub('${e_dt_var}',1)
         union
         select
             robot_id,
             robot_name,
             robot_type_id,
             robot_type_inner_name,
             robot_type_name,
             asset_code,
             asset_type,
             product_type_code,
             product_type_code_name,
             product_id,
             product_id_name,
             assets_status,
             assets_status_name,
             status,
             status_name,
             robot_manufacturer_id,
             robot_manufacturer_name,
             model,
             os,
             version_code,
             software_version,
             hardware_version,
             head_url,
             regist_time,
             bind_status,
             bind_status_name,
             is_hi_service,
             is_privacy_enhance,
             remind_policy_id,
             sku,
             quality_date,
             customer_quality_date,
             out_type_state,
             out_type_state_name,
             product_date,
             in_stock_date,
             out_stock_date,
             in_stock_staff_id,
             in_stock_staff_name,
             out_stock_staff_id,
             out_stock_staff_name,
             note,
             description,
             create_time as start_time,
             '9999-12-31 23:59:59.999' as end_time,
             k8s_env_name,
             create_time,
             update_time
         from inc_data
     )

insert overwrite table cdmdim.dim_pmd_robot_sh_d partition(dt)
select
    robot_id,
    robot_name,
    robot_type_id,
    robot_type_inner_name,
    robot_type_name,
    asset_code,
    asset_type,
    product_type_code,
    product_type_code_name,
    product_id,
    product_id_name,
    assets_status,
    assets_status_name,
    status,
    status_name,
    robot_manufacturer_id,
    robot_manufacturer_name,
    model,
    os,
    version_code,
    software_version,
    hardware_version,
    head_url,
    regist_time,
    bind_status,
    bind_status_name,
    is_hi_service,
    is_privacy_enhance,
    remind_policy_id,
    sku,
    quality_date,
    customer_quality_date,
    out_type_state,
    out_type_state_name,
    product_date,
    in_stock_date,
    out_stock_date,
    in_stock_staff_id,
    in_stock_staff_name,
    out_stock_staff_id,
    out_stock_staff_name,
    note,
    description,
    start_time,
    end_time,
    k8s_env_name,
    create_time,
    update_time,
    '${e_dt_var}' as dt
from union_robot;



-- 17号之后调用的脚本
set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;
set mapreduce.job.queuename=root.users.liuhao;

    -- 获取当前表的最大快照日期，用来判断变化的数据，如果未获取到，需先执行手动脚本初始化数据
with roc_t_dict as (
    select
        t1.robot_type_id,
        t1.robot_type_name,
        t2.robot_type_inner_name
    from (
             select
                 label as robot_type_name,
                 `value` as robot_type_id
             from cdmods.ods_roc_db_c0002_t_dict_s_d
             where dt = '${e_dt_var}' and TYPE = 'robot-type'
             group by label, `value`
         ) t1
             full join (
        select
            label as robot_type_inner_name,
            `value` as robot_type_id
        from cdmods.ods_roc_db_c0002_t_dict_s_d
        where dt = '${e_dt_var}' and TYPE = 'robot-type-hari'
        group by label, `value`
    ) t2 on t1.robot_type_id = t2.robot_type_id
),
-- 机器人制造商表 type=2
     crio_t_dict as (
         select
             trim(code) as robot_manufacturer_id,
             trim(name) as robot_manufacturer_name
         from cdmods.ods_crio_db_c0001_t_dict_s_d where dt = '${e_dt_var}' and `type` = 2
     ),
     crio_t_device as (
         select
             x1.robot_id,
             x1.robot_name,
             x1.robot_type_id,
             nvl(x2.robot_type_inner_name,'') as robot_type_inner_name,
             x1.robot_type_name,
             x1.asset_code,
             x1.asset_type,
             x1.product_type_code,
             x1.product_type_code_name,
             x1.product_id,
             x1.product_id_name,
             x1.assets_status,
             x1.assets_status_name,
             x1.status,
             x1.status_name,
             nvl(x1.robot_manufacturer_id,'') as robot_manufacturer_id,
             case when nvl(x3.robot_manufacturer_name,'') != '' then x3.robot_manufacturer_name
                  else nvl(x1.robot_manufacturer_name,'')
                 end as robot_manufacturer_name,
             x1.model,
             x1.os,
             x1.version_code,
             x1.software_version,
             x1.hardware_version,
             x1.head_url,
             x1.regist_time,
             x1.bind_status,
             x1.bind_status_name,
             x1.is_hi_service,
             x1.is_privacy_enhance,
             x1.remind_policy_id,
             x1.sku,
             x1.quality_date,
             x1.customer_quality_date,
             x1.out_type_state,
             x1.out_type_state_name,
             x1.product_date,
             x1.in_stock_date,
             x1.out_stock_date,
             x1.in_stock_staff_id,
             x1.in_stock_staff_name,
             x1.out_stock_staff_id,
             x1.out_stock_staff_name,
             x1.note,
             x1.description,
             x1.create_time,
             x1.update_time,
             x1.k8s_env_name
         from (
                  select
                      nvl(t1.device_code,'') as robot_id,
                      regexp_replace(nvl(trim(t1.device_name),''), '\r|\n|\t', '') as robot_name,
                      nvl(t2.robot_type_id,'') as robot_type_id,
                      nvl(t2.robot_type_name,'') as robot_type_name,
                      nvl(t1.asset_code,'') as asset_code,
                      nvl(t1.asset_type,'') as asset_type,
                      nvl(t1.product_type_code,'') as product_type_code,
                      nvl(t1.product_type_code_name,'') as product_type_code_name,
                      nvl(t1.product_id,'') as product_id,
                      nvl(t1.product_id_name,'') as product_id_name,
                      nvl(t1.asset_status,-99999998) as assets_status,
                      case when t1.asset_status = 0 then '在册'
                           when t1.asset_status = 1 then '在库'
                           when t1.asset_status = 2 then '出库'
                           else '未知'
                          end as assets_status_name,
                      case when t1.status = 1 then 0
                           when t1.status = 9 then -1
                           else -99999998
                          end as status,
                      case when t1.status = 1 then '正常'
                           when t1.status = 9 then '删除'
                           else '未知'
                          end as status_name,
                      nvl(supplier_code,'') as robot_manufacturer_id,
                      nvl(supplier_code_name,'') as robot_manufacturer_name,
                      nvl(t1.device_model,'') as model,
                      '' as os,
                      '' as version_code,
                      nvl(t1.software_version,'') as software_version,
                      nvl(t1.hardware_version,'') as hardware_version,
                      '' as head_url,
                      '' as regist_time,
                      -99999998 as bind_status,
                      '未知' as bind_status_name,
                      -99999998 as is_hi_service,
                      -99999998 as is_privacy_enhance,
                      '' as remind_policy_id,
                      nvl(t1.sku,'') as sku,
                      case when length(t1.quality_date) = 10 then nvl(concat(from_unixtime( cast(t1.quality_date as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
                          --                            when length(t1.quality_date) = 19 then nvl(concat(from_utc_timestamp(t1.quality_date,'PRC'),'.000'),'')
--                            when length(t1.quality_date) > 19 and length(t1.quality_date) < 23 then if(LENGTH(from_utc_timestamp(t1.quality_date,'PRC'))=19,nvl(concat(from_utc_timestamp(t1.quality_date,'PRC'),'.000'),''),nvl(rpad(from_utc_timestamp(t1.quality_date,'PRC'),23,'0'),''))
--                            when length(t1.quality_date) >= 23 then nvl(from_utc_timestamp(SUBSTRING(REGEXP_REPLACE(t1.quality_date,'T',' '),0,23),'PRC'),'')
                           when length(t1.quality_date) = 19 then nvl(concat(t1.quality_date,'.000'),'')
                           when length(t1.quality_date) > 19 and length(t1.quality_date) < 23 then if(LENGTH(t1.quality_date)=19,nvl(concat(t1.quality_date,'.000'),''),nvl(rpad(t1.quality_date,23,'0'),''))
                           when length(t1.quality_date) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(t1.quality_date,'T',' '),0,23),'')
                           else nvl(concat(from_unixtime( cast(t1.quality_date/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(t1.quality_date,11,3)),'')
                          end as quality_date,
                      case when length(t1.customer_quality_date) = 10 then nvl(concat(from_unixtime( cast(t1.customer_quality_date as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
                          --                            when length(t1.customer_quality_date) = 19 then nvl(concat(from_utc_timestamp(t1.customer_quality_date,'PRC'),'.000'),'')
--                            when length(t1.customer_quality_date) > 19 and length(t1.customer_quality_date) < 23 then if(LENGTH(from_utc_timestamp(t1.customer_quality_date,'PRC'))=19,nvl(concat(from_utc_timestamp(t1.customer_quality_date,'PRC'),'.000'),''),nvl(rpad(from_utc_timestamp(t1.customer_quality_date,'PRC'),23,'0'),''))
--                            when length(t1.customer_quality_date) >= 23 then nvl(from_utc_timestamp(SUBSTRING(REGEXP_REPLACE(t1.customer_quality_date,'T',' '),0,23),'PRC'),'')
                           when length(t1.customer_quality_date) = 19 then nvl(concat(t1.customer_quality_date,'.000'),'')
                           when length(t1.customer_quality_date) > 19 and length(t1.customer_quality_date) < 23 then if(LENGTH(t1.customer_quality_date)=19,nvl(concat(t1.customer_quality_date,'.000'),''),nvl(rpad(t1.customer_quality_date,23,'0'),''))
                           when length(t1.customer_quality_date) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(t1.customer_quality_date,'T',' '),0,23),'')
                           else nvl(concat(from_unixtime( cast(t1.customer_quality_date/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(t1.customer_quality_date,11,3)),'')
                          end as customer_quality_date,
                      -99999998 as out_type_state,
                      '未知' as out_type_state_name,
                      case when length(t1.product_date) = 10 then nvl(concat(from_unixtime( cast(t1.product_date as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
                          --                            when length(t1.product_date) = 19 then nvl(concat(from_utc_timestamp(t1.product_date,'PRC'),'.000'),'')
--                            when length(t1.product_date) > 19 and length(t1.product_date) < 23 then if(LENGTH(from_utc_timestamp(t1.product_date,'PRC'))=19,nvl(concat(from_utc_timestamp(t1.product_date,'PRC'),'.000'),''),nvl(rpad(from_utc_timestamp(t1.product_date,'PRC'),23,'0'),''))
--                            when length(t1.product_date) >= 23 then nvl(from_utc_timestamp(SUBSTRING(REGEXP_REPLACE(t1.product_date,'T',' '),0,23),'PRC'),'')
                           when length(t1.product_date) = 19 then nvl(concat(t1.product_date,'.000'),'')
                           when length(t1.product_date) > 19 and length(t1.product_date) < 23 then if(LENGTH(t1.product_date)=19,nvl(concat(t1.product_date,'.000'),''),nvl(rpad(t1.product_date,23,'0'),''))
                           when length(t1.product_date) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(t1.product_date,'T',' '),0,23),'')
                           else nvl(concat(from_unixtime( cast(t1.product_date/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(t1.product_date,11,3)),'')
                          end as product_date,
                      '' as in_stock_date,
                      '' as out_stock_date,
                      '' as in_stock_staff_id,
                      '' as in_stock_staff_name,
                      '' as out_stock_staff_id,
                      '' as out_stock_staff_name,
                      '' as note,
                      '' as description,
                      case when length(t1.create_time) = 10 then nvl(concat(from_unixtime( cast(t1.create_time as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
                          --                            when length(t1.create_time) = 19 then nvl(concat(from_utc_timestamp(t1.create_time,'PRC'),'.000'),'')
--                            when length(t1.create_time) > 19 and length(t1.create_time) < 23 then if(LENGTH(from_utc_timestamp(t1.create_time,'PRC'))=19,nvl(concat(from_utc_timestamp(t1.create_time,'PRC'),'.000'),''),nvl(rpad(from_utc_timestamp(t1.create_time,'PRC'),23,'0'),''))
--                            when length(t1.create_time) >= 23 then nvl(from_utc_timestamp(SUBSTRING(REGEXP_REPLACE(t1.create_time,'T',' '),0,23),'PRC'),'')
                           when length(t1.create_time) = 19 then nvl(concat(t1.create_time,'.000'),'')
                           when length(t1.create_time) > 19 and length(t1.create_time) < 23 then if(LENGTH(t1.create_time)=19,nvl(concat(t1.create_time,'.000'),''),nvl(rpad(t1.create_time,23,'0'),''))
                           when length(t1.create_time) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(t1.create_time,'T',' '),0,23),'')
                           else nvl(concat(from_unixtime( cast(t1.create_time/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(t1.create_time,11,3)),'')
                          end as create_time,
                      case when length(t1.update_time) = 10 then nvl(concat(from_unixtime( cast(t1.update_time as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
                          --                            when length(t1.update_time) = 19 then nvl(concat(from_utc_timestamp(t1.update_time,'PRC'),'.000'),'')
--                            when length(t1.update_time) > 19 and length(t1.update_time) < 23 then if(LENGTH(from_utc_timestamp(t1.update_time,'PRC'))=19,nvl(concat(from_utc_timestamp(t1.update_time,'PRC'),'.000'),''),nvl(rpad(from_utc_timestamp(t1.update_time,'PRC'),23,'0'),''))
--                            when length(t1.update_time) >= 23 then nvl(from_utc_timestamp(SUBSTRING(REGEXP_REPLACE(t1.update_time,'T',' '),0,23),'PRC'),'')
                           when length(t1.update_time) = 19 then nvl(concat(t1.update_time,'.000'),'')
                           when length(t1.update_time) > 19 and length(t1.update_time) < 23 then if(LENGTH(t1.update_time)=19,nvl(concat(t1.update_time,'.000'),''),nvl(rpad(t1.update_time,23,'0'),''))
                           when length(t1.update_time) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(t1.update_time,'T',' '),0,23),'')
                           else nvl(concat(from_unixtime( cast(t1.update_time/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(t1.update_time,11,3)),'')
                          end as update_time,
                      'bj-prod-232' as k8s_env_name
                  from cdmods.ods_crio_db_c0001_t_device_i_d t1
                           left join (
                      select
                          robot_type_id,
                          robot_type_name
                      from roc_t_dict
                  ) t2 on trim(lower(regexp_replace(t1.product_type_code,'\\s|-',''))) = trim(lower(regexp_replace(t2.robot_type_name,'\\s|-','')))
                  where dt >= '${s_dt_var}' and dt <= '${e_dt_var}'
              ) x1 left join (
             select
                 robot_type_id,
                 robot_type_inner_name
             from roc_t_dict
         ) x2 on x1.robot_type_id = x2.robot_type_id
                   left join crio_t_dict x3 on nvl(trim(x1.robot_manufacturer_id),'') = x3.robot_manufacturer_id
     ),
     inc_data as (
         select
             robot_id,
             robot_name,
             robot_type_id,
             robot_type_inner_name,
             robot_type_name,
             asset_code,
             asset_type,
             product_type_code,
             product_type_code_name,
             product_id,
             product_id_name,
             assets_status,
             assets_status_name,
             status,
             status_name,
             robot_manufacturer_id,
             robot_manufacturer_name,
             model,
             os,
             version_code,
             software_version,
             hardware_version,
             head_url,
             regist_time,
             bind_status,
             bind_status_name,
             is_hi_service,
             is_privacy_enhance,
             remind_policy_id,
             sku,
             quality_date,
             customer_quality_date,
             out_type_state,
             out_type_state_name,
             product_date,
             in_stock_date,
             out_stock_date,
             in_stock_staff_id,
             in_stock_staff_name,
             out_stock_staff_id,
             out_stock_staff_name,
             note,
             description,
             create_time as start_time,
             '9999-12-31 23:59:59.999' as end_time,
             k8s_env_name,
             create_time,
             update_time
         from (
                  select
                      t1.robot_id,
                      t1.robot_name,
                      t1.robot_type_id,
                      t1.robot_type_inner_name,
                      t1.robot_type_name,
                      t1.asset_code,
                      t1.asset_type,
                      t1.product_type_code,
                      t1.product_type_code_name,
                      t1.product_id,
                      t1.product_id_name,
                      t1.assets_status,
                      t1.assets_status_name,
                      t1.status,
                      t1.status_name,
                      t1.robot_manufacturer_id,
                      t1.robot_manufacturer_name,
                      t1.model,
                      t1.os,
                      t1.version_code,
                      t1.software_version,
                      t1.hardware_version,
                      t1.head_url,
                      t1.regist_time,
                      t1.bind_status,
                      t1.bind_status_name,
                      t1.is_hi_service,
                      t1.is_privacy_enhance,
                      t1.remind_policy_id,
                      t1.sku,
                      t1.quality_date,
                      t1.customer_quality_date,
                      t1.out_type_state,
                      t1.out_type_state_name,
                      t1.product_date,
                      t1.in_stock_date,
                      t1.out_stock_date,
                      t1.in_stock_staff_id,
                      t1.in_stock_staff_name,
                      t1.out_stock_staff_id,
                      t1.out_stock_staff_name,
                      t1.note,
                      t1.description,
                      t1.create_time,
                      t1.update_time,
                      t1.k8s_env_name,
                      row_number() OVER (PARTITION BY t1.robot_id ORDER BY create_time asc) as rnk
                  from crio_t_device t1
                           left join (
                      select
                          robot_id
                      from cdmdim.dim_pmd_robot_sh_d where dt = date_sub('${e_dt_var}',1) group by robot_id
                  ) t2 on t1.robot_id = t2.robot_id
                  where t1.robot_id is not null and t2.robot_id is null
              ) x1 where x1.rnk = 1
     ),
     union_robot as (
         select
             robot_id,
             robot_name,
             robot_type_id,
             robot_type_inner_name,
             robot_type_name,
             asset_code,
             asset_type,
             product_type_code,
             product_type_code_name,
             product_id,
             product_id_name,
             assets_status,
             assets_status_name,
             status,
             status_name,
             robot_manufacturer_id,
             robot_manufacturer_name,
             model,
             os,
             version_code,
             software_version,
             hardware_version,
             head_url,
             regist_time,
             bind_status,
             bind_status_name,
             is_hi_service,
             is_privacy_enhance,
             remind_policy_id,
             sku,
             quality_date,
             customer_quality_date,
             out_type_state,
             out_type_state_name,
             product_date,
             in_stock_date,
             out_stock_date,
             in_stock_staff_id,
             in_stock_staff_name,
             out_stock_staff_id,
             out_stock_staff_name,
             note,
             description,
             start_time,
             end_time,
             k8s_env_name,
             create_time,
             update_time
         from cdmdim.dim_pmd_robot_sh_d where dt = date_sub('${e_dt_var}',1)
         union
         select
             robot_id,
             robot_name,
             robot_type_id,
             robot_type_inner_name,
             robot_type_name,
             asset_code,
             asset_type,
             product_type_code,
             product_type_code_name,
             product_id,
             product_id_name,
             assets_status,
             assets_status_name,
             status,
             status_name,
             robot_manufacturer_id,
             robot_manufacturer_name,
             model,
             os,
             version_code,
             software_version,
             hardware_version,
             head_url,
             regist_time,
             bind_status,
             bind_status_name,
             is_hi_service,
             is_privacy_enhance,
             remind_policy_id,
             sku,
             quality_date,
             customer_quality_date,
             out_type_state,
             out_type_state_name,
             product_date,
             in_stock_date,
             out_stock_date,
             in_stock_staff_id,
             in_stock_staff_name,
             out_stock_staff_id,
             out_stock_staff_name,
             note,
             description,
             create_time as start_time,
             '9999-12-31 23:59:59.999' as end_time,
             k8s_env_name,
             create_time,
             update_time
         from inc_data
     )

insert overwrite table cdmdim.dim_pmd_robot_sh_d partition(dt)
select
    robot_id,
    robot_name,
    robot_type_id,
    robot_type_inner_name,
    robot_type_name,
    asset_code,
    asset_type,
    product_type_code,
    product_type_code_name,
    product_id,
    product_id_name,
    assets_status,
    assets_status_name,
    status,
    status_name,
    robot_manufacturer_id,
    robot_manufacturer_name,
    model,
    os,
    version_code,
    software_version,
    hardware_version,
    head_url,
    regist_time,
    bind_status,
    bind_status_name,
    is_hi_service,
    is_privacy_enhance,
    remind_policy_id,
    sku,
    quality_date,
    customer_quality_date,
    out_type_state,
    out_type_state_name,
    product_date,
    in_stock_date,
    out_stock_date,
    in_stock_staff_id,
    in_stock_staff_name,
    out_stock_staff_id,
    out_stock_staff_name,
    note,
    description,
    start_time,
    end_time,
    k8s_env_name,
    create_time,
    update_time,
    '${e_dt_var}' as dt
from union_robot;





