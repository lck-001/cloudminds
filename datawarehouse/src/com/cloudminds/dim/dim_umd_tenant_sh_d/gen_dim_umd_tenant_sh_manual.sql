set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;
set mapreduce.job.queuename=root.users.liuhao;

with roc_tenant as (
    select
        tenant_id,
        tenant_name,
        tenant_type,
        tenant_type_name,
        industry_id,
        device_num,
        region,
        address,
        contact,
        phone,
        telephone,
        email,
        -99999998 as boss_status,
        '未知' as boss_status_name,
        roc_status,
        roc_status_name,
        charge_time,
        expired_time,
        logo,
        brand,
        is_need_vpn,
        '' as service_address,
        description,
        create_time,
        update_time,
        k8s_env_name,
        '' as country,
        '' as province,
        '' as city,
        '' as phone_area_code,
        '' as project_name,
        '' as district,
        '' as vop_id,
        '' as customer_id,
        '' as sub_sys,
        '' as time_zone,
        '' as net_env_id,
        '' as mcs_area,
        '' as seller,
        dt
    from(
        select
            nvl(tenant_code,'') as tenant_id,
            nvl(tenant_name,'') as tenant_name,
            nvl(tenant_type,-99999998) as tenant_type,
            case tenant_type when 0 then '长租' when 1 then '短租' when 2 then'试用' when 3 then '售卖' when 4 then '测试' else '未知' end as tenant_type_name,
            nvl(industry,'') as industry_id,
            nvl(vpn_user_limit,-99999998)  as device_num,
            nvl(region,'') as region,
            nvl(address,'') as address,
            nvl(contacts,'') as contact,
            nvl(phone_code,'') as phone,
            nvl(telephone,'') as telephone,
            nvl(email,'') as email,
            nvl(status,-99999998) as roc_status,
            case status when -1 then '删除' when 0 then '正常' when 1 then '待开通' when 2 then '锁定' else '未知' END as roc_status_name,
            case when length(open_time) = 10 then nvl(concat(from_unixtime( cast(open_time as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
                  when length(open_time) = 19 then nvl(concat(open_time,'.000'),'')
                  when length(open_time) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(open_time,'T',' '),0,23),'')
                  else nvl(concat(from_unixtime( cast(open_time/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(open_time,11,3)),'')
            end as charge_time,
            case when length(expired_time) = 10 then nvl(concat(from_unixtime( cast(expired_time as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
                  when length(expired_time) = 19 then nvl(concat(expired_time,'.000'),'')
                  when length(expired_time) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(expired_time,'T',' '),0,23),'')
                  else nvl(concat(from_unixtime( cast(expired_time/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(expired_time,11,3)),'')
            end as expired_time,
            nvl(logo,'') as logo,
            nvl(branding,'') as brand,
            nvl(need_vpn,-99999998) as is_need_vpn,
            regexp_replace(nvl(trim(description),''), '\r|\n|\t', '') as description,
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
            nvl(k8s_env_name,'') as k8s_env_name,
            dt
        from cdmods.ods_roc_db_c0002_t_tenant_s_d where dt <= '${e_dt_var}'
        union
        select
            nvl(tenant_code,'') as tenant_id,
            nvl(tenant_name,'') as tenant_name,
            nvl(tenant_type,-99999998) as tenant_type,
            case tenant_type when 0 then '长租' when 1 then '短租' when 2 then '试用' when 3 then '售卖' when 4 then '测试' else '未知' end as tenant_type_name,
            nvl(industry,'') as industry_id,
            nvl(vpn_user_limit,-99999998)  as device_num,
            nvl(region,'') as region,
            nvl(address,'') as address,
            nvl(contacts,'') as contact,
            nvl(phone_code,'') as phone,
            nvl(telephone,'') as telephone,
            nvl(email,'') as email,
            nvl(status,-99999998) as roc_status,
            case status when -1 then '删除' when 0 then '正常' when 1 then '待开通' when 2 then '锁定' else '未知' END as roc_status_name,
            case when length(open_time) = 10 then nvl(concat(from_unixtime( cast(open_time as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
                  when length(open_time) = 19 then nvl(concat(open_time,'.000'),'')
                  when length(open_time) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(open_time,'T',' '),0,23),'')
                  else nvl(concat(from_unixtime( cast(open_time/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(open_time,11,3)),'')
            end as charge_time,
            case when length(expired_time) = 10 then nvl(concat(from_unixtime( cast(expired_time as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
                  when length(expired_time) = 19 then nvl(concat(expired_time,'.000'),'')
                  when length(expired_time) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(expired_time,'T',' '),0,23),'')
                  else nvl(concat(from_unixtime( cast(expired_time/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(expired_time,11,3)),'')
            end as expired_time,
            nvl(logo,'') as logo,
            nvl(branding,'') as brand,
            nvl(need_vpn,-99999998) as is_need_vpn,
            regexp_replace(nvl(trim(description),''), '\r|\n|\t', '') as description,
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
            nvl(k8s_env_name,'') as k8s_env_name,
            dt
        from cdmods.ods_roc_db_c0008_t_tenant_s_d where dt <= '${e_dt_var}'
    ) as rt where substring(rt.update_time,0,10) <= '${e_dt_var}'
),
boss_tenant as (
    select
        tenant_id,
        tenant_name,
        tenant_type,
        tenant_type_name,
        industry_id,
        device_num,
        '' as region,
        address,
        contact,
        phone,
        '' as telephone,
        email,
        boss_status,
        boss_status_name,
        -99999998 as roc_status,
        '未知' as roc_status_name,
        '' as charge_time,
        '' as expired_time,
        logo,
        '' as brand,
        -99999998 as is_need_vpn,
        service_address,
        description,
        create_time,
        update_time,
        k8s_env_name,
        '' as country,
        '' as province,
        '' as city,
        '' as phone_area_code,
        '' as project_name,
        '' as district,
        '' as vop_id,
        customer_id,
        sub_sys,
        time_zone,
        net_env_id,
        mcs_area,
        seller,
        dt
    from (
        select
            nvl(code,'') as tenant_id,
            nvl(cast(customer_id as string),'') as customer_id,
            nvl(sub_business,'') as sub_sys,
            nvl(time_zone,'') as time_zone,
            nvl(environment,'') as net_env_id,
            nvl(vpn_zone_codes,'') as mcs_area,
            nvl(salesman,'') as seller,
            regexp_replace(nvl(trim(description),''), '\r|\n|\t', '') as description,
            nvl(cast(status as int),-99999998) as boss_status,
            case when status = 1 then '有效'
                 when status = 9 then '删除'
                 else '未知'
            end as boss_status_name,
            nvl(service_address,'') as service_address,
            nvl(name,'') as tenant_name,
            nvl(industry,'') as industry_id,
            nvl(cast(type as int),-99999998) as tenant_type,
            case type when '0' then '长租' when '1' then '短租' when '2' then'试用' when '3' then '售卖' when '4' then '测试' else '未知' end as tenant_type_name,
            nvl(phone_code,'') as phone,
            nvl(address,'') as address,
            nvl(vpn_user_limit,-99999998)  as device_num,
            nvl(email,'') as email,
            nvl(log,'') as logo,
            nvl(contacts,'') as contact,
            case when length(create_time) = 10 then nvl(concat(from_unixtime( cast(create_time as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
                  when length(create_time) = 19 then nvl(concat(from_utc_timestamp(create_time,'PRC'),'.000'),'')
                  when length(create_time) >= 23 then nvl(from_utc_timestamp(SUBSTRING(REGEXP_REPLACE(create_time,'T',' '),0,23),'PRC'),'')
--                  when length(create_time) = 19 then nvl(concat(create_time,'.000'),'')
--                  when length(create_time) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(create_time,'T',' '),0,23),'')
                  else ''
            end as create_time,
            case when length(update_time) = 10 then nvl(concat(from_unixtime( cast(update_time as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
                 when length(update_time) = 19 then nvl(concat(from_utc_timestamp(update_time,'PRC'),'.000'),'')
                 when length(update_time) >= 23 then nvl(from_utc_timestamp(SUBSTRING(REGEXP_REPLACE(update_time,'T',' '),0,23),'PRC'),'')
--                  when length(update_time) = 19 then nvl(concat(update_time,'.000'),'')
--                  when length(update_time) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(update_time,'T',' '),0,23),'')
                 else ''
            end as update_time,
            case when trim(environment) = 'PROD251' then 'bj-prod-251'
                 when trim(environment) = 'Singapore' then 'Singapore'
                 when trim(environment) = 'Development' then 'Development'
                 else 'bj-prod-232'
            end as k8s_env_name,
            dt
        from cdmods.ods_crio_db_c0001_tenant_s_d where dt <= '${e_dt_var}'
    ) bt where substring(bt.update_time,0,10) <= '${e_dt_var}'
),
-- 可能出现手动更改数据库，导致update时间未变化
--    CM-Show	公司外演示	2	试用	1	253							0	正常	2021-07-05 18:35:12.000				1		2021-07-05 18:35:12.000	2021-10-19 00:52:19.000	bj-prod-232
--    CM-Show	公司外演示	2	试用	1	200							0	正常	2021-07-05 18:35:12.000				1		2021-07-05 18:35:12.000	2021-10-19 00:52:19.000	bj-prod-232
-- 按照update_time排序,取最新分区
union_roc_boss as (
    select
        tenant_id,
        tenant_name,
        tenant_type,
        tenant_type_name,
        industry_id,
        device_num,
        region,
        address,
        contact,
        phone,
        telephone,
        email,
        boss_status,
        boss_status_name,
        roc_status,
        roc_status_name,
        charge_time,
        expired_time,
        logo,
        brand,
        is_need_vpn,
        service_address,
        description,
        create_time,
        update_time,
        k8s_env_name,
        country,
        province,
        city,
        phone_area_code,
        project_name,
        district,
        vop_id,
        customer_id,
        sub_sys,
        time_zone,
        net_env_id,
        mcs_area,
        seller
    from (
         select
             tenant_id,
             tenant_name,
             tenant_type,
             tenant_type_name,
             industry_id,
             device_num,
             region,
             address,
             contact,
             phone,
             telephone,
             email,
             boss_status,
             boss_status_name,
             roc_status,
             roc_status_name,
             charge_time,
             expired_time,
             logo,
             brand,
             is_need_vpn,
             service_address,
             description,
             create_time,
             update_time,
             k8s_env_name,
             country,
             province,
             city,
             phone_area_code,
             project_name,
             district,
             vop_id,
             customer_id,
             sub_sys,
             time_zone,
             net_env_id,
             mcs_area,
             seller,
             row_number() OVER (PARTITION BY tenant_id,update_time ORDER BY dt DESC) as rnk
         from (
              select
                  tenant_id,
                  tenant_name,
                  tenant_type,
                  tenant_type_name,
                  industry_id,
                  device_num,
                  region,
                  address,
                  contact,
                  phone,
                  telephone,
                  email,
                  boss_status,
                  boss_status_name,
                  roc_status,
                  roc_status_name,
                  charge_time,
                  expired_time,
                  logo,
                  brand,
                  is_need_vpn,
                  service_address,
                  description,
                  create_time,
                  update_time,
                  k8s_env_name,
                  country,
                  province,
                  city,
                  phone_area_code,
                  project_name,
                  district,
                  vop_id,
                  customer_id,
                  sub_sys,
                  time_zone,
                  net_env_id,
                  mcs_area,
                  seller,
                  dt
              from boss_tenant
              union
              select
                  tenant_id,
                  tenant_name,
                  tenant_type,
                  tenant_type_name,
                  industry_id,
                  device_num,
                  region,
                  address,
                  contact,
                  phone,
                  telephone,
                  email,
                  boss_status,
                  boss_status_name,
                  roc_status,
                  roc_status_name,
                  charge_time,
                  expired_time,
                  logo,
                  brand,
                  is_need_vpn,
                  service_address,
                  description,
                  create_time,
                  update_time,
                  k8s_env_name,
                  country,
                  province,
                  city,
                  phone_area_code,
                  project_name,
                  district,
                  vop_id,
                  customer_id,
                  sub_sys,
                  time_zone,
                  net_env_id,
                  mcs_area,
                  seller,
                  dt
              from roc_tenant
          ) t
    ) t1 where t1.rnk = 1
),
-- union_roc_boss as (
--     select
--         case when t1.tenant_id is not null and t2.tenant_id is not null then t1.tenant_id
--              else coalesce(t1.tenant_id,t2.tenant_id)
--         end as tenant_id,
--         case when t1.tenant_id is not null and t2.tenant_id is not null then if(nvl(t1.tenant_name,'') != '',t1.tenant_name,nvl(t2.tenant_name,''))
--              else coalesce(t1.tenant_name,t2.tenant_name,'')
--         end as tenant_name,
--         case when t1.tenant_id is not null and t2.tenant_id is not null then if(nvl(t1.tenant_type,-99999998) != -99999998,t1.tenant_type,nvl(t2.tenant_type,-99999998))
--              else coalesce(t1.tenant_type,t2.tenant_type,-99999998)
--         end as tenant_type,
--         case when t1.tenant_id is not null and t2.tenant_id is not null then if(nvl(t1.tenant_type_name,'未知') != '未知',t1.tenant_type_name,nvl(t2.tenant_type_name,'未知'))
--              else coalesce(t1.tenant_type_name,t2.tenant_type_name,'未知')
--         end as tenant_type_name,
--         case when t1.tenant_id is not null and t2.tenant_id is not null then if(nvl(t1.industry_id,'') != '',t1.industry_id,nvl(t2.industry_id,''))
--              else coalesce(t1.industry_id,t2.industry_id,'')
--         end as industry_id,
--         case when t1.tenant_id is not null and t2.tenant_id is not null then if(nvl(t1.device_num,-99999998) != -99999998,t1.device_num,nvl(t2.device_num,-99999998))
--              else coalesce(t1.device_num,t2.device_num,-99999998)
--         end as device_num,
--         case when t1.tenant_id is not null and t2.tenant_id is not null then if(nvl(t1.region,'') != '',t1.region,nvl(t2.region,''))
--              else coalesce(t1.region,t2.region,'')
--         end as region,
--         case when t1.tenant_id is not null and t2.tenant_id is not null then if(nvl(t1.address,'') != '',t1.address,nvl(t2.address,''))
--              else coalesce(t1.address,t2.address,'')
--         end as address,
--         case when t1.tenant_id is not null and t2.tenant_id is not null then if(nvl(t1.contact,'') != '',t1.contact,nvl(t2.contact,''))
--              else coalesce(t1.contact,t2.contact,'')
--         end as contact,
--         case when t1.tenant_id is not null and t2.tenant_id is not null then if(nvl(t1.phone,'') != '',t1.phone,nvl(t2.phone,''))
--              else coalesce(t1.phone,t2.phone,'')
--         end as phone,
--         case when t1.tenant_id is not null and t2.tenant_id is not null then if(nvl(t1.telephone,'') != '',t1.telephone,nvl(t2.telephone,''))
--              else coalesce(t1.telephone,t2.telephone,'')
--         end as telephone,
--         case when t1.tenant_id is not null and t2.tenant_id is not null then if(nvl(t1.email,'') != '',t1.email,nvl(t2.email,''))
--              else coalesce(t1.email,t2.email,'')
--         end as email,
--         case when t1.tenant_id is not null and t2.tenant_id is not null then if(nvl(t1.status,-99999998) != -99999998,t1.status,nvl(t2.status,-99999998))
--              else coalesce(t1.status,t2.status,-99999998)
--         end as status,
--         case when t1.tenant_id is not null and t2.tenant_id is not null then if(nvl(t1.status_name,'未知') != '未知',t1.status_name,nvl(t2.status_name,'未知'))
--              else coalesce(t1.status_name,t2.status_name,'未知')
--         end as status_name,
--         case when t1.tenant_id is not null and t2.tenant_id is not null then if(nvl(t1.charge_time,'') != '',t1.charge_time,nvl(t2.charge_time,''))
--              else coalesce(t1.charge_time,t2.charge_time,'')
--         end as charge_time,
--         case when t1.tenant_id is not null and t2.tenant_id is not null then if(nvl(t1.expired_time,'') != '',t1.expired_time,nvl(t2.expired_time,''))
--              else coalesce(t1.expired_time,t2.expired_time,'')
--         end as expired_time,
--         case when t1.tenant_id is not null and t2.tenant_id is not null then if(nvl(t1.logo,'') != '',t1.logo,nvl(t2.logo,''))
--              else coalesce(t1.logo,t2.logo,'')
--         end as logo,
--         case when t1.tenant_id is not null and t2.tenant_id is not null then if(nvl(t1.brand,'') != '',t1.brand,nvl(t2.brand,''))
--              else coalesce(t1.brand,t2.brand,'')
--         end as brand,
--         case when t1.tenant_id is not null and t2.tenant_id is not null then if(nvl(t1.is_need_vpn,-99999998) != -99999998,t1.is_need_vpn,nvl(t2.is_need_vpn,-99999998))
--              else coalesce(t1.is_need_vpn,t2.is_need_vpn,-99999998)
--         end as is_need_vpn,
--         case when t1.tenant_id is not null and t2.tenant_id is not null then if(nvl(t1.description,'') != '',t1.description,nvl(t2.description,''))
--              else coalesce(t1.description,t2.description,'')
--         end as description,
--         case when t1.tenant_id is not null and t2.tenant_id is not null then if(nvl(t1.create_time,'') != '',t1.create_time,nvl(t2.create_time,''))
--              else coalesce(t1.create_time,t2.create_time,'')
--         end as create_time,
--         case when t1.tenant_id is not null and t2.tenant_id is not null then if(nvl(t1.update_time,'') != '',t1.update_time,nvl(t2.update_time,''))
--              else coalesce(t1.update_time,t2.update_time,'')
--         end as update_time,
--         case when t1.tenant_id is not null and t2.tenant_id is not null then if(nvl(t1.k8s_env_name,'') != '',t1.k8s_env_name,nvl(t2.k8s_env_name,''))
--              else coalesce(t1.k8s_env_name,t2.k8s_env_name,'')
--         end as k8s_env_name,
--         case when t1.tenant_id is not null and t2.tenant_id is not null then if(nvl(t1.customer_id,'') != '',t1.customer_id,nvl(t2.customer_id,''))
--              else coalesce(t1.customer_id,t2.customer_id,'')
--         end as customer_id,
--         case when t1.tenant_id is not null and t2.tenant_id is not null then if(nvl(t1.sub_sys,'') != '',t1.sub_sys,nvl(t2.sub_sys,''))
--              else coalesce(t1.sub_sys,t2.sub_sys,'')
--         end as sub_sys,
--         case when t1.tenant_id is not null and t2.tenant_id is not null then if(nvl(t1.time_zone,'') != '',t1.time_zone,nvl(t2.time_zone,''))
--              else coalesce(t1.time_zone,t2.time_zone,'')
--         end as time_zone,
--         case when t1.tenant_id is not null and t2.tenant_id is not null then if(nvl(t1.net_env_id,'') != '',t1.net_env_id,nvl(t2.net_env_id,''))
--              else coalesce(t1.net_env_id,t2.net_env_id,'')
--         end as net_env_id,
--         case when t1.tenant_id is not null and t2.tenant_id is not null then if(nvl(t1.mcs_area,'') != '',t1.mcs_area,nvl(t2.mcs_area,''))
--              else coalesce(t1.mcs_area,t2.mcs_area,'')
--         end as mcs_area,
--         case when t1.tenant_id is not null and t2.tenant_id is not null then if(nvl(t1.seller,'') != '',t1.seller,nvl(t2.seller,''))
--              else coalesce(t1.seller,t2.seller,'')
--         end as seller
--     from roc_tenant t1 full join boss_tenant t2
--     on t1.tenant_id = t2.tenant_id and t1.update_time = t2.update_time
-- ),
un_join_data as (
    select
        case when t1.tenant_id is not null and t2.tenant_id is null then nvl(t1.tenant_id,'')
             else nvl(t2.tenant_id,'')
        end as tenant_id,
        case when t1.tenant_id is not null and t2.tenant_id is null then nvl(t1.tenant_name,'')
             else nvl(t2.tenant_name,'')
        end as tenant_name,
        case when t1.tenant_id is not null and t2.tenant_id is null then nvl(t1.tenant_type,-99999998)
             else nvl(t2.tenant_type,-99999998)
        end as tenant_type,
        case when t1.tenant_id is not null and t2.tenant_id is null then nvl(t1.tenant_type_name,'未知')
             else nvl(t2.tenant_type_name,'未知')
        end as tenant_type_name,
        case when t1.tenant_id is not null and t2.tenant_id is null then nvl(t1.industry_id,'')
             else nvl(t2.industry_id,'')
        end as industry_id,
        case when t1.tenant_id is not null and t2.tenant_id is null then nvl(t1.device_num,-99999998)
             else nvl(t2.device_num,-99999998)
        end as device_num,
        case when t1.tenant_id is not null and t2.tenant_id is null then nvl(t1.region,'')
             else nvl(t2.region,'')
        end as region,
        case when t1.tenant_id is not null and t2.tenant_id is null then nvl(t1.address,'')
             else nvl(t2.address,'')
        end as address,
        case when t1.tenant_id is not null and t2.tenant_id is null then nvl(t1.contact,'')
             else nvl(t2.contact,'')
        end as contact,
        case when t1.tenant_id is not null and t2.tenant_id is null then nvl(t1.phone,'')
             else nvl(t2.phone,'')
        end as phone,
        case when t1.tenant_id is not null and t2.tenant_id is null then nvl(t1.telephone,'')
             else nvl(t2.telephone,'')
        end as telephone,
        case when t1.tenant_id is not null and t2.tenant_id is null then nvl(t1.email,'')
             else nvl(t2.email,'')
        end as email,
        case when t1.tenant_id is not null and t2.tenant_id is null then nvl(t1.boss_status,-99999998)
             else nvl(t2.boss_status,-99999998)
        end as boss_status,
        case when t1.tenant_id is not null and t2.tenant_id is null then nvl(t1.boss_status_name,'未知')
             else nvl(t2.boss_status_name,'未知')
        end as boss_status_name,
        case when t1.tenant_id is not null and t2.tenant_id is null then nvl(t1.roc_status,-99999998)
             else nvl(t2.roc_status,-99999998)
        end as roc_status,
        case when t1.tenant_id is not null and t2.tenant_id is null then nvl(t1.roc_status_name,'未知')
             else nvl(t2.roc_status_name,'未知')
        end as roc_status_name,
        case when t1.tenant_id is not null and t2.tenant_id is null then nvl(t1.charge_time,'')
             else nvl(t2.charge_time,'')
        end as charge_time,
        case when t1.tenant_id is not null and t2.tenant_id is null then nvl(t1.expired_time,'')
             else nvl(t2.expired_time,'')
        end as expired_time,
        case when t1.tenant_id is not null and t2.tenant_id is null then nvl(t1.logo,'')
             else nvl(t2.logo,'')
        end as logo,
        case when t1.tenant_id is not null and t2.tenant_id is null then nvl(t1.brand,'')
             else nvl(t2.brand,'')
        end as brand,
        case when t1.tenant_id is not null and t2.tenant_id is null then nvl(t1.is_need_vpn,-99999998)
             else nvl(t2.is_need_vpn,-99999998)
        end as is_need_vpn,
        case when t1.tenant_id is not null and t2.tenant_id is null then nvl(t1.service_address,'')
             else nvl(t2.service_address,'')
        end as service_address,
        case when t1.tenant_id is not null and t2.tenant_id is null then nvl(t1.description,'')
             else nvl(t2.description,'')
        end as description,
        case when t1.tenant_id is not null and t2.tenant_id is null then nvl(t1.create_time,'')
             else nvl(t2.create_time,'')
        end as create_time,
        case when t1.tenant_id is not null and t2.tenant_id is null then nvl(t1.update_time,'')
             else nvl(t2.update_time,'')
        end as update_time,
        case when t1.tenant_id is not null and t2.tenant_id is null then nvl(t1.k8s_env_name,'')
             else nvl(t2.k8s_env_name,'')
        end as k8s_env_name,
        case when t1.tenant_id is not null and t2.tenant_id is null then nvl(t1.country,'')
             else nvl(t2.country,'')
        end as country,
        case when t1.tenant_id is not null and t2.tenant_id is null then nvl(t1.province,'')
             else nvl(t2.province,'')
        end as province,
        case when t1.tenant_id is not null and t2.tenant_id is null then nvl(t1.city,'')
             else nvl(t2.city,'')
        end as city,
        case when t1.tenant_id is not null and t2.tenant_id is null then nvl(t1.phone_area_code,'')
             else nvl(t2.phone_area_code,'')
        end as phone_area_code,
        case when t1.tenant_id is not null and t2.tenant_id is null then nvl(t1.project_name,'')
             else nvl(t2.project_name,'')
        end as project_name,
        case when t1.tenant_id is not null and t2.tenant_id is null then nvl(t1.district,'')
             else nvl(t2.district,'')
        end as district,
        case when t1.tenant_id is not null and t2.tenant_id is null then nvl(t1.vop_id,'')
             else nvl(t2.vop_id,'')
        end as vop_id,
        case when t1.tenant_id is not null and t2.tenant_id is null then nvl(t1.customer_id,'')
             else nvl(t2.customer_id,'')
        end as customer_id,
        case when t1.tenant_id is not null and t2.tenant_id is null then nvl(t1.sub_sys,'')
             else nvl(t2.sub_sys,'')
        end as sub_sys,
        case when t1.tenant_id is not null and t2.tenant_id is null then nvl(t1.time_zone,'')
             else nvl(t2.time_zone,'')
        end as time_zone,
        case when t1.tenant_id is not null and t2.tenant_id is null then nvl(t1.net_env_id,'')
             else nvl(t2.net_env_id,'')
        end as net_env_id,
        case when t1.tenant_id is not null and t2.tenant_id is null then nvl(t1.mcs_area,'')
             else nvl(t2.mcs_area,'')
        end as mcs_area,
        case when t1.tenant_id is not null and t2.tenant_id is null then nvl(t1.seller,'')
             else nvl(t2.seller,'')
        end as seller,
        case when t1.tenant_id is not null and t2.tenant_id is null then nvl(t1.update_time,'')
             else nvl(t2.update_time,'')
        end as start_time,
        '9999-12-31 23:59:59.999' as end_time
    from (
         select
             tenant_id,
             tenant_name,
             tenant_type,
             tenant_type_name,
             industry_id,
             device_num,
             region,
             address,
             contact,
             phone,
             telephone,
             email,
             boss_status,
             boss_status_name,
             roc_status,
             roc_status_name,
             charge_time,
             expired_time,
             logo,
             brand,
             is_need_vpn,
             service_address,
             description,
             create_time,
             update_time,
             k8s_env_name,
             country,
             province,
             city,
             phone_area_code,
             project_name,
             district,
             vop_id,
             customer_id,
             sub_sys,
             time_zone,
             net_env_id,
             mcs_area,
             seller
         from union_roc_boss
         ) t1 left join (
        select
            tenant_id,
            tenant_name,
            tenant_type,
            tenant_type_name,
            industry_id,
            device_num,
            region,
            address,
            contact,
            phone,
            telephone,
            email,
            boss_status,
            boss_status_name,
            roc_status,
            roc_status_name,
            charge_time,
            expired_time,
            logo,
            brand,
            is_need_vpn,
            service_address,
            description,
            create_time,
            update_time,
            k8s_env_name,
            country,
            province,
            city,
            phone_area_code,
            project_name,
            district,
            vop_id,
            customer_id,
            sub_sys,
            time_zone,
            net_env_id,
            mcs_area,
            seller,
            start_time,
            end_time
        from cdmdim.dim_umd_tenant_sh_d where dt = date_sub('${e_dt_var}',1) and end_time = '9999-12-31 23:59:59.999'
--         from cdmtmp.tmp_dim_umd_tenant_sh_d_luojun_20220105 where dt = date_sub('${e_dt_var}',1) and end_time = '9999-12-31 23:59:59.999'
    ) t2 on t1.tenant_id = t2.tenant_id
    where t2.tenant_id is null
),
join_data as (
    select
        tenant_id,
        tenant_name,
        tenant_type,
        tenant_type_name,
        industry_id,
        device_num,
        region,
        address,
        contact,
        phone,
        telephone,
        email,
        boss_status,
        boss_status_name,
        roc_status,
        roc_status_name,
        charge_time,
        expired_time,
        logo,
        brand,
        is_need_vpn,
        service_address,
        description,
        create_time,
        update_time,
        k8s_env_name,
        country,
        province,
        city,
        phone_area_code,
        project_name,
        district,
        vop_id,
        customer_id,
        sub_sys,
        time_zone,
        net_env_id,
        mcs_area,
        seller,
        start_time,
        end_time
    from cdmdim.dim_umd_tenant_sh_d where dt = date_sub('${e_dt_var}',1)
--     from cdmtmp.tmp_dim_umd_tenant_sh_d_luojun_20220105 where dt = date_sub('${e_dt_var}',1)
    union
    select
        coalesce(if(nvl(t2.tenant_id,'') = '',null,t2.tenant_id),if(nvl(t1.tenant_id,'') = '',null,t1.tenant_id),'') as tenant_id,
        coalesce(if(nvl(t2.tenant_name,'') = '',null,t2.tenant_name),if(nvl(t1.tenant_name,'') = '',null,t1.tenant_name),'') as tenant_name,
        coalesce(if(nvl(t2.tenant_type,-99999998) = -99999998,null,t2.tenant_type),if(nvl(t1.tenant_type,-99999998) = -99999998,null,t1.tenant_type),-99999998) as tenant_type,
        coalesce(if(nvl(t2.tenant_type_name,'未知') = '未知',null,t2.tenant_type_name),if(nvl(t1.tenant_type_name,'未知') = '未知',null,t1.tenant_type_name),'未知') as tenant_type_name,
        coalesce(if(nvl(t2.industry_id,'') = '',null,t2.industry_id),if(nvl(t1.industry_id,'') = '',null,t1.industry_id),'') as industry_id,
        coalesce(if(nvl(t2.device_num,-99999998) = -99999998,null,t2.device_num),if(nvl(t1.device_num, -99999998) = -99999998,null,t1.device_num),-99999998) as device_num,
        coalesce(if(nvl(t2.region,'') = '',null,t2.region),if(nvl(t1.region,'') = '',null,t1.region),'') as region,
        coalesce(if(nvl(t2.address,'') = '',null,t2.address),if(nvl(t1.address,'') = '',null,t1.address),'') as address,
        coalesce(if(nvl(t2.contact,'') = '',null,t2.contact),if(nvl(t1.contact,'') = '',null,t1.contact),'') as contact,
        coalesce(if(nvl(t2.phone,'') = '',null,t2.phone),if(nvl(t1.phone,'') = '',null,t1.phone),'') as phone,
        coalesce(if(nvl(t2.telephone,'') = '',null,t2.telephone),if(nvl(t1.telephone,'') = '',null,t1.telephone),'') as telephone,
        coalesce(if(nvl(t2.email,'') = '',null,t2.email),if(nvl(t1.email,'') = '',null,t1.email),'') as email,
        coalesce(if(nvl(t2.boss_status,-99999998) = -99999998,null,t2.boss_status),if(nvl(t1.boss_status,-99999998) = -99999998,null,t1.boss_status),-99999998) as boss_status,
        coalesce(if(nvl(t2.boss_status_name,'未知') = '未知',null,t2.boss_status_name),if(nvl(t1.boss_status_name,'未知') = '未知',null,t1.boss_status_name),'未知') as boss_status_name,
        coalesce(if(nvl(t2.roc_status,-99999998) = -99999998,null,t2.roc_status),if(nvl(t1.roc_status,-99999998) = -99999998,null,t1.roc_status),-99999998) as roc_status,
        coalesce(if(nvl(t2.roc_status_name,'未知') = '未知',null,t2.roc_status_name),if(nvl(t1.roc_status_name,'未知') = '未知',null,t1.roc_status_name),'未知') as roc_status_name,
        coalesce(if(nvl(t2.charge_time,'') = '',null,t2.charge_time),if(nvl(t1.charge_time,'') = '',null,t1.charge_time),'') as charge_time,
        coalesce(if(nvl(t2.expired_time,'') = '',null,t2.expired_time),if(nvl(t1.expired_time,'') = '',null,t1.expired_time),'') as expired_time,
        coalesce(if(nvl(t2.logo,'') = '',null,t2.logo),if(nvl(t1.logo,'') = '',null,t1.logo),'') as logo,
        coalesce(if(nvl(t2.brand,'') = '',null,t2.brand),if(nvl(t1.brand,'') = '',null,t1.brand),'') as brand,
        coalesce(if(nvl(t2.is_need_vpn,-99999998) = -99999998,null,t2.is_need_vpn),if(nvl(t1.is_need_vpn,-99999998) = -99999998,null,t1.is_need_vpn),-99999998) as is_need_vpn,
        coalesce(if(nvl(t2.service_address,'') = '',null,t2.service_address),if(nvl(t1.service_address,'') = '',null,t1.service_address),'') as service_address,
        coalesce(if(nvl(t2.description,'') = '',null,t2.description),if(nvl(t1.description,'') = '',null,t1.description),'') as description,
        coalesce(if(nvl(t2.create_time,'') = '',null,t2.create_time),if(nvl(t1.create_time,'') = '',null,t1.create_time),'') as create_time,
        coalesce(if(nvl(t2.update_time,'') = '',null,t2.update_time),if(nvl(t1.update_time,'') = '',null,t1.update_time),'') as update_time,
        coalesce(if(nvl(t2.k8s_env_name,'') = '',null,t2.k8s_env_name),if(nvl(t1.k8s_env_name,'') = '',null,t1.k8s_env_name),'') as k8s_env_name,
        coalesce(if(nvl(t2.country,'') = '',null,t2.country),if(nvl(t1.country,'') = '',null,t1.country),'') as country,
        coalesce(if(nvl(t2.province,'') = '',null,t2.province),if(nvl(t1.province,'') = '',null,t1.province),'') as province,
        coalesce(if(nvl(t2.city,'') = '',null,t2.city),if(nvl(t1.city,'') = '',null,t1.city),'') as city,
        coalesce(if(nvl(t2.phone_area_code,'') = '',null,t2.phone_area_code),if(nvl(t1.phone_area_code,'') = '',null,t1.phone_area_code),'') as phone_area_code,
        coalesce(if(nvl(t2.project_name,'') = '',null,t2.project_name),if(nvl(t1.project_name,'') = '',null,t1.project_name),'') as project_name,
        coalesce(if(nvl(t2.district,'') = '',null,t2.district),if(nvl(t1.district,'') = '',null,t1.district),'') as district,
        coalesce(if(nvl(t2.vop_id,'') = '',null,t2.vop_id),if(nvl(t1.vop_id,'') = '',null,t1.vop_id),'') as vop_id,
        coalesce(if(nvl(t2.customer_id,'') = '',null,t2.customer_id),if(nvl(t1.customer_id,'') = '',null,t1.customer_id),'') as customer_id,
        coalesce(if(nvl(t2.sub_sys,'') = '',null,t2.sub_sys),if(nvl(t1.sub_sys,'') = '',null,t1.sub_sys),'') as sub_sys,
        coalesce(if(nvl(t2.time_zone,'') = '',null,t2.time_zone),if(nvl(t1.time_zone,'') = '',null,t1.time_zone),'') as time_zone,
        coalesce(if(nvl(t2.net_env_id,'') = '',null,t2.net_env_id),if(nvl(t1.net_env_id,'') = '',null,t1.net_env_id),'') as net_env_id,
        coalesce(if(nvl(t2.mcs_area,'') = '',null,t2.mcs_area),if(nvl(t1.mcs_area,'') = '',null,t1.mcs_area),'') as mcs_area,
        coalesce(if(nvl(t2.seller,'') = '',null,t2.seller),if(nvl(t1.seller,'') = '',null,t1.seller),'') as seller,
        coalesce(if(nvl(t2.update_time,'') = '',null,t2.update_time),if(nvl(t1.update_time,'') = '',null,t1.update_time),'') as start_time,
        '9999-12-31 23:59:59.999' as end_time
    from(
    select
        tenant_id,
        tenant_name,
        tenant_type,
        tenant_type_name,
        industry_id,
        device_num,
        region,
        address,
        contact,
        phone,
        telephone,
        email,
        boss_status,
        boss_status_name,
        roc_status,
        roc_status_name,
        charge_time,
        expired_time,
        logo,
        brand,
        is_need_vpn,
        service_address,
        description,
        create_time,
        update_time,
        k8s_env_name,
        country,
        province,
        city,
        phone_area_code,
        project_name,
        district,
        vop_id,
        customer_id,
        sub_sys,
        time_zone,
        net_env_id,
        mcs_area,
        seller,
        start_time,
        end_time
    from cdmdim.dim_umd_tenant_sh_d where dt = date_sub('${e_dt_var}',1) and end_time = '9999-12-31 23:59:59.999'
--     from cdmtmp.tmp_dim_umd_tenant_sh_d_luojun_20220105 where dt = date_sub('${e_dt_var}',1) and end_time = '9999-12-31 23:59:59.999'
    ) t1 left join (
        select
            tenant_id,
            tenant_name,
            tenant_type,
            tenant_type_name,
            industry_id,
            device_num,
            region,
            address,
            contact,
            phone,
            telephone,
            email,
            boss_status,
            boss_status_name,
            roc_status,
            roc_status_name,
            charge_time,
            expired_time,
            logo,
            brand,
            is_need_vpn,
            service_address,
            description,
            create_time,
            update_time,
            k8s_env_name,
            country,
            province,
            city,
            phone_area_code,
            project_name,
            district,
            vop_id,
            customer_id,
            sub_sys,
            time_zone,
            net_env_id,
            mcs_area,
            seller
        from union_roc_boss
    ) t2 on t1.tenant_id = t2.tenant_id
    where t1.tenant_id is not null and t2.tenant_id is not null
),
merge_data as (
    select
        tenant_id,
        tenant_name,
        tenant_type,
        tenant_type_name,
        industry_id,
        device_num,
        region,
        address,
        contact,
        phone,
        telephone,
        email,
        boss_status,
        boss_status_name,
        roc_status,
        roc_status_name,
        charge_time,
        expired_time,
        logo,
        brand,
        is_need_vpn,
        service_address,
        description,
        create_time,
        update_time,
        k8s_env_name,
        country,
        province,
        city,
        phone_area_code,
        project_name,
        district,
        vop_id,
        customer_id,
        sub_sys,
        time_zone,
        net_env_id,
        mcs_area,
        seller,
        start_time,
        CASE WHEN tmp_end_time IS NOT NULL AND tmp_end_time != '' THEN concat(from_unixtime(cast((cast(unix_timestamp(substring(tmp_end_time,0,19),'yyyy-MM-dd HH:mm:ss') AS bigint)*1000 + cast(substring(tmp_end_time,21,23) AS bigint) - 1)/1000 as bigint),'yyyy-MM-dd HH:mm:ss'),'.',SUBSTRING((cast(unix_timestamp(substring(tmp_end_time,0,19),'yyyy-MM-dd HH:mm:ss') AS bigint)*1000 + cast(substring(tmp_end_time,21,23) AS bigint) - 1),11,13))
             ELSE '9999-12-31 23:59:59.999'
        END AS end_time
   from(
    select
        tenant_id,
        tenant_name,
        tenant_type,
        tenant_type_name,
        industry_id,
        device_num,
        region,
        address,
        contact,
        phone,
        telephone,
        email,
        boss_status,
        boss_status_name,
        roc_status,
        roc_status_name,
        charge_time,
        expired_time,
        logo,
        brand,
        is_need_vpn,
        service_address,
        description,
        create_time,
        update_time,
        k8s_env_name,
        country,
        province,
        city,
        phone_area_code,
        project_name,
        district,
        vop_id,
        customer_id,
        sub_sys,
        time_zone,
        net_env_id,
        mcs_area,
        seller,
        start_time,
        end_time,
        lead(update_time,1,NULL) over(partition BY tenant_id ORDER BY update_time ASC) tmp_end_time
    from join_data
    ) t1
    union
    select
        tenant_id,
        tenant_name,
        tenant_type,
        tenant_type_name,
        industry_id,
        device_num,
        region,
        address,
        contact,
        phone,
        telephone,
        email,
        boss_status,
        boss_status_name,
        roc_status,
        roc_status_name,
        charge_time,
        expired_time,
        logo,
        brand,
        is_need_vpn,
        service_address,
        description,
        create_time,
        update_time,
        k8s_env_name,
        country,
        province,
        city,
        phone_area_code,
        project_name,
        district,
        vop_id,
        customer_id,
        sub_sys,
        time_zone,
        net_env_id,
        mcs_area,
        seller,
        start_time,
        CASE WHEN tmp_end_time IS NOT NULL AND tmp_end_time != '' THEN concat(from_unixtime(cast((cast(unix_timestamp(substring(tmp_end_time,0,19),'yyyy-MM-dd HH:mm:ss') AS bigint)*1000 + cast(substring(tmp_end_time,21,23) AS bigint) - 1)/1000 as bigint),'yyyy-MM-dd HH:mm:ss'),'.',SUBSTRING((cast(unix_timestamp(substring(tmp_end_time,0,19),'yyyy-MM-dd HH:mm:ss') AS bigint)*1000 + cast(substring(tmp_end_time,21,23) AS bigint) - 1),11,13))
             ELSE '9999-12-31 23:59:59.999'
        END AS end_time
    from(
    select
        tenant_id,
        tenant_name,
        tenant_type,
        tenant_type_name,
        industry_id,
        device_num,
        region,
        address,
        contact,
        phone,
        telephone,
        email,
        boss_status,
        boss_status_name,
        roc_status,
        roc_status_name,
        charge_time,
        expired_time,
        logo,
        brand,
        is_need_vpn,
        service_address,
        description,
        create_time,
        update_time,
        k8s_env_name,
        country,
        province,
        city,
        phone_area_code,
        project_name,
        district,
        vop_id,
        customer_id,
        sub_sys,
        time_zone,
        net_env_id,
        mcs_area,
        seller,
        start_time,
        end_time,
        lead(update_time,1,NULL) over(partition BY tenant_id ORDER BY update_time ASC) tmp_end_time
    from un_join_data
    )t2
),
column_dim as (
    select
        tenant_id,
        collect_set(case when tenant_name is not null and tenant_name != '' then concat_ws('->',tenant_id,end_time,tenant_name) else null end ) as tenant_name_dim,
        collect_set(case when tenant_type is not null and tenant_type != -99999998 then concat_ws('->',tenant_id,end_time,cast(tenant_type as string)) else null end ) as tenant_type_dim,
        collect_set(case when industry_id is not null and industry_id != '' then concat_ws('->',tenant_id,end_time,industry_id) else null end ) as industry_id_dim,
        collect_set(case when device_num is not null and device_num != -99999998 then concat_ws('->',tenant_id,end_time,cast(device_num as string)) else null end ) as device_num_dim,
        collect_set(case when region is not null and region != '' then concat_ws('->',tenant_id,end_time,region) else null end ) as region_dim,
        collect_set(case when address is not null and address != '' then concat_ws('->',tenant_id,end_time,address) else null end ) as address_dim,
        collect_set(case when contact is not null and contact != '' then concat_ws('->',tenant_id,end_time,contact) else null end ) as contact_dim,
        collect_set(case when phone is not null and phone != '' then concat_ws('->',tenant_id,end_time,phone) else null end ) as phone_dim,
        collect_set(case when telephone is not null and telephone != '' then concat_ws('->',tenant_id,end_time,telephone) else null end ) as telephone_dim,
        collect_set(case when email is not null and email != '' then concat_ws('->',tenant_id,end_time,email) else null end ) as email_dim,
        collect_set(case when boss_status is not null and boss_status != -99999998 then concat_ws('->',tenant_id,end_time,cast(boss_status as string)) else null end ) as boss_status_dim,
        collect_set(case when roc_status is not null and roc_status != -99999998 then concat_ws('->',tenant_id,end_time,cast(roc_status as string)) else null end ) as roc_status_dim,
        collect_set(case when charge_time is not null and charge_time != '' then concat_ws('->',tenant_id,end_time,charge_time) else null end ) as charge_time_dim,
        collect_set(case when expired_time is not null and expired_time != '' then concat_ws('->',tenant_id,end_time,expired_time) else null end ) as expired_time_dim,
        collect_set(case when logo is not null and logo != '' then concat_ws('->',tenant_id,end_time,logo) else null end ) as logo_dim,
        collect_set(case when brand is not null and brand != '' then concat_ws('->',tenant_id,end_time,brand) else null end ) as brand_dim,
        collect_set(case when is_need_vpn is not null and is_need_vpn != -99999998 then concat_ws('->',tenant_id,end_time,cast(is_need_vpn as string)) else null end ) as is_need_vpn_dim,
        collect_set(case when service_address is not null and service_address != '' then concat_ws('->',tenant_id,end_time,service_address) else null end ) as service_address_dim,
        collect_set(case when description is not null and description != '' then concat_ws('->',tenant_id,end_time,description) else null end ) as description_dim,
        collect_set(case when country is not null and country != '' then concat_ws('->',tenant_id,end_time,country) else null end ) as country_dim,
        collect_set(case when province is not null and province != '' then concat_ws('->',tenant_id,end_time,province) else null end ) as province_dim,
        collect_set(case when city is not null and city != '' then concat_ws('->',tenant_id,end_time,city) else null end ) as city_dim,
        collect_set(case when phone_area_code is not null and phone_area_code != '' then concat_ws('->',tenant_id,end_time,phone_area_code) else null end ) as phone_area_code_dim,
        collect_set(case when project_name is not null and project_name != '' then concat_ws('->',tenant_id,end_time,project_name) else null end ) as project_name_dim,
        collect_set(case when district is not null and district != '' then concat_ws('->',tenant_id,end_time,district) else null end ) as district_dim,
        collect_set(case when vop_id is not null and vop_id != '' then concat_ws('->',tenant_id,end_time,vop_id) else null end ) as vop_id_dim,
        collect_set(case when customer_id is not null and customer_id != '' then concat_ws('->',tenant_id,end_time,customer_id) else null end ) as customer_id_dim,
        collect_set(case when sub_sys is not null and sub_sys != '' then concat_ws('->',tenant_id,end_time,sub_sys) else null end ) as sub_sys_dim,
        collect_set(case when time_zone is not null and time_zone != '' then concat_ws('->',tenant_id,end_time,time_zone) else null end ) as time_zone_dim,
        collect_set(case when net_env_id is not null and net_env_id != '' then concat_ws('->',tenant_id,end_time,net_env_id) else null end ) as net_env_id_dim,
        collect_set(case when mcs_area is not null and mcs_area != '' then concat_ws('->',tenant_id,end_time,mcs_area) else null end ) as mcs_area_dim,
        collect_set(case when seller is not null and seller != '' then concat_ws('->',tenant_id,end_time,seller) else null end ) as seller_dim
    from merge_data
    group by tenant_id
),
dim_repair as (
    select
        x1.tenant_id as tenant_id,
        case when nvl(x1.tenant_name_dim,'') != '' then x1.tenant_name_dim
             else x1.tenant_name
        end as tenant_name,
        case when nvl(x1.tenant_type_dim,'') != '' then cast(x1.tenant_type_dim as int)
             else nvl(x1.tenant_type,-99999998)
        end as tenant_type,
        case when nvl(x1.tenant_type_dim,'')!= '' and cast(x1.tenant_type_dim as int) = 0 then '长租'
             when nvl(x1.tenant_type_dim,'')!= '' and cast(x1.tenant_type_dim as int) = 1 then '短租'
             when nvl(x1.tenant_type_dim,'')!= '' and cast(x1.tenant_type_dim as int) = 2 then '试用'
             when nvl(x1.tenant_type_dim,'')!= '' and cast(x1.tenant_type_dim as int) = 3 then '售卖'
             when nvl(x1.tenant_type_dim,'')!= '' and cast(x1.tenant_type_dim as int) = 4 then '测试'
             else '未知'
        end as tenant_type_name,
        case when nvl(x1.industry_id_dim,'') != '' then x1.industry_id_dim
             else x1.industry_id
        end as industry_id,
        case when nvl(x1.device_num_dim,'') != '' then cast(x1.device_num_dim as int)
             else nvl(x1.device_num,-99999998)
        end as device_num,
        case when nvl(x1.region_dim,'') != '' then x1.region_dim
             else x1.region
        end as region,
        case when nvl(x1.address_dim,'') != '' then x1.address_dim
             else x1.address
        end as address,
        case when nvl(x1.contact_dim,'') != '' then x1.contact_dim
             else x1.contact
        end as contact,
        case when nvl(x1.phone_dim,'') != '' then x1.phone_dim
             else x1.phone
        end as phone,
        case when nvl(x1.telephone_dim,'') != '' then x1.telephone_dim
             else x1.telephone
        end as telephone,
        case when nvl(x1.email_dim,'') != '' then x1.email_dim
             else x1.email
        end as email,
        case when nvl(x1.boss_status_dim,'') != '' then cast(x1.boss_status_dim as int)
             else nvl(x1.boss_status,-99999998)
        end as boss_status,
        case when nvl(x1.boss_status_dim,'') != '' and cast(x1.boss_status_dim as int) = 1 then '有效'
             when nvl(x1.boss_status_dim,'') != '' and cast(x1.boss_status_dim as int) = 9 then '删除'
             else '未知'
        end as boss_status_name,
        case when nvl(x1.roc_status_dim,'') != '' then cast(x1.roc_status_dim as int)
             else nvl(x1.roc_status,-99999998)
        end as roc_status,
        case when nvl(x1.roc_status_dim,'') != '' and cast(x1.roc_status_dim as int) = 0 then '正常'
             when nvl(x1.roc_status_dim,'') != '' and cast(x1.roc_status_dim as int) = 1 then '待开通'
             when nvl(x1.roc_status_dim,'') != '' and cast(x1.roc_status_dim as int) = -1 then '删除'
             when nvl(x1.roc_status_dim,'') != '' and cast(x1.roc_status_dim as int) = 2 then '锁定'
             else '未知'
        end as roc_status_name,
        case when nvl(x1.charge_time_dim,'') != '' then x1.charge_time_dim
             else x1.charge_time
        end as charge_time,
        case when nvl(x1.expired_time_dim,'') != '' then x1.expired_time_dim
             else x1.expired_time
        end as expired_time,
        case when nvl(x1.logo_dim,'') != '' then x1.logo_dim
             else x1.logo
        end as logo,
        case when nvl(x1.brand_dim,'') != '' then x1.brand_dim
             else x1.brand
        end as brand,
        case when nvl(x1.is_need_vpn_dim,'') != '' then cast(x1.is_need_vpn_dim as int)
             else nvl(x1.is_need_vpn,-99999998)
        end as is_need_vpn,
        case when nvl(x1.service_address_dim,'') != '' then x1.service_address_dim
             else x1.service_address
        end as service_address,
        case when nvl(x1.description_dim,'') != '' then x1.description_dim
             else x1.description
        end as description,
        case when nvl(x1.country_dim,'') != '' then x1.country_dim
             else x1.country
        end as country,
        case when nvl(x1.province_dim,'') != '' then x1.province_dim
             else x1.province
        end as province,
        case when nvl(x1.city_dim,'') != '' then x1.city_dim
             else x1.city
        end as city,
        case when nvl(x1.phone_area_code_dim,'') != '' then x1.phone_area_code_dim
             else x1.phone_area_code
        end as phone_area_code,
        case when nvl(x1.project_name_dim,'') != '' then x1.project_name_dim
             else x1.project_name
        end as project_name,
        case when nvl(x1.district_dim,'') != '' then x1.district_dim
             else x1.district
        end as district,
        case when nvl(x1.vop_id_dim,'') != '' then x1.vop_id_dim
             else x1.vop_id
        end as vop_id,
        case when nvl(x1.customer_id_dim,'') != '' then x1.customer_id_dim
             else x1.customer_id
        end as customer_id,
        case when nvl(x1.sub_sys_dim,'') != '' then x1.sub_sys_dim
             else x1.sub_sys
        end as sub_sys,
        case when nvl(x1.time_zone_dim,'') != '' then x1.time_zone_dim
             else x1.time_zone
        end as time_zone,
        case when nvl(x1.net_env_id_dim,'') != '' then x1.net_env_id_dim
             else x1.net_env_id
        end as net_env_id,
        case when nvl(x1.mcs_area_dim,'') != '' then x1.mcs_area_dim
             else x1.mcs_area
        end as mcs_area,
        case when nvl(x1.seller_dim,'') != '' then x1.seller_dim
             else x1.seller
        end as seller,
        x1.start_time as start_time,
        x1.end_time as end_time,
        x1.k8s_env_name as k8s_env_name,
        x1.create_time as create_time,
        x1.update_time as update_time,
        row_number() OVER (PARTITION BY x1.tenant_id ORDER BY x1.update_time asc) as rnk
    from(
        select
            t1.tenant_id as tenant_id,
            t1.tenant_name as tenant_name,
            t1.tenant_type as tenant_type,
            t1.tenant_type_name as tenant_type_name,
            t1.industry_id as industry_id,
            t1.device_num as device_num,
            t1.region as region,
            t1.address as address,
            t1.contact as contact,
            t1.phone as phone,
            t1.telephone as telephone,
            t1.email as email,
            t1.boss_status as boss_status,
            t1.boss_status_name as boss_status_name,
            t1.roc_status as roc_status,
            t1.roc_status_name as roc_status_name,
            t1.charge_time as charge_time,
            t1.expired_time as expired_time,
            t1.logo as logo,
            t1.brand as brand,
            t1.is_need_vpn as is_need_vpn,
            t1.service_address as service_address,
            t1.description as description,
            t1.create_time as create_time,
            t1.update_time as update_time,
            t1.k8s_env_name as k8s_env_name,
            t1.country as country,
            t1.province as province,
            t1.city as city,
            t1.phone_area_code as phone_area_code,
            t1.project_name as project_name,
            t1.district as district,
            t1.vop_id as vop_id,
            t1.customer_id as customer_id,
            t1.sub_sys as sub_sys,
            t1.time_zone as time_zone,
            t1.net_env_id as net_env_id,
            t1.mcs_area as mcs_area,
            t1.seller as seller,
            t1.start_time as start_time,
            t1.end_time as end_time,
            case when t2.tenant_id is not null and size(t2.tenant_name_dim) > 0 then cdmudf.dim(t1.end_time,t2.tenant_name_dim)
            else ''
            end as tenant_name_dim,
            case when t2.tenant_id is not null and size(t2.tenant_type_dim) > 0 then cdmudf.dim(t1.end_time,t2.tenant_type_dim)
            else ''
            end as tenant_type_dim,
            case when t2.tenant_id is not null and size(t2.industry_id_dim) > 0 then cdmudf.dim(t1.end_time,t2.industry_id_dim)
            else ''
            end as industry_id_dim,
            case when t2.tenant_id is not null and size(t2.device_num_dim) > 0 then cdmudf.dim(t1.end_time,t2.device_num_dim)
            else ''
            end as device_num_dim,
            case when t2.tenant_id is not null and size(t2.region_dim) > 0 then cdmudf.dim(t1.end_time,t2.region_dim)
            else ''
            end as region_dim,
            case when t2.tenant_id is not null and size(t2.address_dim) > 0 then cdmudf.dim(t1.end_time,t2.address_dim)
            else ''
            end as address_dim,
            case when t2.tenant_id is not null and size(t2.contact_dim) > 0 then cdmudf.dim(t1.end_time,t2.contact_dim)
            else ''
            end as contact_dim,
            case when t2.tenant_id is not null and size(t2.phone_dim) > 0 then cdmudf.dim(t1.end_time,t2.phone_dim)
            else ''
            end as phone_dim,
            case when t2.tenant_id is not null and size(t2.telephone_dim) > 0 then cdmudf.dim(t1.end_time,t2.telephone_dim)
            else ''
            end as telephone_dim,
            case when t2.tenant_id is not null and size(t2.email_dim) > 0 then cdmudf.dim(t1.end_time,t2.email_dim)
            else ''
            end as email_dim,
            case when t2.tenant_id is not null and size(t2.boss_status_dim) > 0 then cdmudf.dim(t1.end_time,t2.boss_status_dim)
            else ''
            end as boss_status_dim,
            case when t2.tenant_id is not null and size(t2.roc_status_dim) > 0 then cdmudf.dim(t1.end_time,t2.roc_status_dim)
            else ''
            end as roc_status_dim,
            case when t2.tenant_id is not null and size(t2.charge_time_dim) > 0 then cdmudf.dim(t1.end_time,t2.charge_time_dim)
            else ''
            end as charge_time_dim,
            case when t2.tenant_id is not null and size(t2.expired_time_dim) > 0 then cdmudf.dim(t1.end_time,t2.expired_time_dim)
            else ''
            end as expired_time_dim,
            case when t2.tenant_id is not null and size(t2.logo_dim) > 0 then cdmudf.dim(t1.end_time,t2.logo_dim)
            else ''
            end as logo_dim,
            case when t2.tenant_id is not null and size(t2.brand_dim) > 0 then cdmudf.dim(t1.end_time,t2.brand_dim)
            else ''
            end as brand_dim,
            case when t2.tenant_id is not null and size(t2.is_need_vpn_dim) > 0 then cdmudf.dim(t1.end_time,t2.is_need_vpn_dim)
            else ''
            end as is_need_vpn_dim,
            case when t2.tenant_id is not null and size(t2.service_address_dim) > 0 then cdmudf.dim(t1.end_time,t2.service_address_dim)
            else ''
            end as service_address_dim,
            case when t2.tenant_id is not null and size(t2.description_dim) > 0 then cdmudf.dim(t1.end_time,t2.description_dim)
            else ''
            end as description_dim,
            case when t2.tenant_id is not null and size(t2.country_dim) > 0 then cdmudf.dim(t1.end_time,t2.country_dim)
            else ''
            end as country_dim,
            case when t2.tenant_id is not null and size(t2.province_dim) > 0 then cdmudf.dim(t1.end_time,t2.province_dim)
            else ''
            end as province_dim,
            case when t2.tenant_id is not null and size(t2.city_dim) > 0 then cdmudf.dim(t1.end_time,t2.city_dim)
            else ''
            end as city_dim,
            case when t2.tenant_id is not null and size(t2.phone_area_code_dim) > 0 then cdmudf.dim(t1.end_time,t2.phone_area_code_dim)
            else ''
            end as phone_area_code_dim,
            case when t2.tenant_id is not null and size(t2.project_name_dim) > 0 then cdmudf.dim(t1.end_time,t2.project_name_dim)
            else ''
            end as project_name_dim,
            case when t2.tenant_id is not null and size(t2.district_dim) > 0 then cdmudf.dim(t1.end_time,t2.district_dim)
            else ''
            end as district_dim,
            case when t2.tenant_id is not null and size(t2.vop_id_dim) > 0 then cdmudf.dim(t1.end_time,t2.vop_id_dim)
            else ''
            end as vop_id_dim,
            case when t2.tenant_id is not null and size(t2.customer_id_dim) > 0 then cdmudf.dim(t1.end_time,t2.customer_id_dim)
            else ''
            end as customer_id_dim,
            case when t2.tenant_id is not null and size(t2.sub_sys_dim) > 0 then cdmudf.dim(t1.end_time,t2.sub_sys_dim)
            else ''
            end as sub_sys_dim,
            case when t2.tenant_id is not null and size(t2.time_zone_dim) > 0 then cdmudf.dim(t1.end_time,t2.time_zone_dim)
            else ''
            end as time_zone_dim,
            case when t2.tenant_id is not null and size(t2.net_env_id_dim) > 0 then cdmudf.dim(t1.end_time,t2.net_env_id_dim)
            else ''
            end as net_env_id_dim,
            case when t2.tenant_id is not null and size(t2.mcs_area_dim) > 0 then cdmudf.dim(t1.end_time,t2.mcs_area_dim)
            else ''
            end as mcs_area_dim,
            case when t2.tenant_id is not null and size(t2.seller_dim) > 0 then cdmudf.dim(t1.end_time,t2.seller_dim)
            else ''
            end as seller_dim
        from merge_data t1
        left join column_dim t2 on t1.tenant_id = t2.tenant_id
    ) x1
),
state_continue as (
    select
        tenant_id,
        tenant_name,
        tenant_type,
        tenant_type_name,
        industry_id,
        device_num,
        region,
        address,
        contact,
        phone,
        telephone,
        email,
        boss_status,
        boss_status_name,
        roc_status,
        roc_status_name,
        charge_time,
        expired_time,
        logo,
        brand,
        is_need_vpn,
        service_address,
        description,
        country,
        province,
        city,
        phone_area_code,
        project_name,
        district,
        vop_id,
        customer_id,
        sub_sys,
        time_zone,
        net_env_id,
        mcs_area,
        seller,
        min(start_time) as start_time,
        max(end_time) as end_time,
        create_time,
        min(update_time) as update_time,
        k8s_env_name,
        row_number() OVER (PARTITION BY tenant_id ORDER BY min(update_time) asc) as rnk
   from(
       select
            tenant_id,
            tenant_name,
            tenant_type,
            tenant_type_name,
            industry_id,
            device_num,
            region,
            address,
            contact,
            phone,
            telephone,
            email,
            boss_status,
            boss_status_name,
            roc_status,
            roc_status_name,
            charge_time,
            expired_time,
            logo,
            brand,
            is_need_vpn,
            service_address,
            description,
            create_time,
            update_time,
            k8s_env_name,
            country,
            province,
            city,
            phone_area_code,
            project_name,
            district,
            vop_id,
            customer_id,
            sub_sys,
            time_zone,
            net_env_id,
            mcs_area,
            seller,
            start_time,
            end_time,
            row_number() over(partition by tenant_id order by end_time asc) as rnk1,
            row_number() over(partition by tenant_id,tenant_name,tenant_type,tenant_type_name,industry_id,device_num,region,address,contact,phone,telephone,email,boss_status,boss_status_name,roc_status,roc_status_name,charge_time,expired_time,logo,brand,is_need_vpn,service_address,description,create_time,k8s_env_name,country,province,city,phone_area_code,project_name,district,vop_id,customer_id,sub_sys,time_zone,net_env_id,mcs_area,seller order by end_time asc) as rnk2
       from dim_repair
   ) t group by tenant_id,tenant_name,tenant_type,tenant_type_name,industry_id,device_num,region,address,contact,phone,telephone,email,boss_status,boss_status_name,roc_status,roc_status_name,charge_time,expired_time,logo,brand,is_need_vpn,service_address,description,create_time,k8s_env_name,country,province,city,phone_area_code,project_name,district,vop_id,customer_id,sub_sys,time_zone,net_env_id,mcs_area,seller,rnk2-rnk1
),
modify_start_time as (
    select
        tenant_id,
        tenant_name,
        tenant_type,
        tenant_type_name,
        industry_id,
        device_num,
        region,
        address,
        contact,
        phone,
        telephone,
        email,
        boss_status,
        boss_status_name,
        roc_status,
        roc_status_name,
        charge_time,
        expired_time,
        logo,
        brand,
        is_need_vpn,
        service_address,
        description,
        create_time,
        update_time,
        k8s_env_name,
        country,
        province,
        city,
        phone_area_code,
        project_name,
        district,
        vop_id,
        customer_id,
        sub_sys,
        time_zone,
        net_env_id,
        mcs_area,
        seller,
        case when rnk = 1 then create_time
             else start_time
        end as start_time,
        end_time,
        k8s_env_name
    from state_continue
)
insert overwrite table cdmdim.dim_umd_tenant_sh_d partition(dt)
-- insert overwrite table cdmtmp.tmp_dim_umd_tenant_sh_d_luojun_20220105 partition(dt)
    select
        tenant_id,
        tenant_name,
        tenant_type,
        tenant_type_name,
        vop_id,
        customer_id,
        industry_id,
        sub_sys,
        time_zone,
        net_env_id,
        mcs_area,
        device_num,
        region,
        country,
        province,
        city,
        address,
        contact,
        phone,
        phone_area_code,
        telephone,
        project_name,
        district,
        seller,
        email,
        boss_status,
        boss_status_name,
        roc_status,
        roc_status_name,
        charge_time,
        expired_time,
        logo,
        description,
        brand,
        is_need_vpn,
        service_address,
        create_time,
        update_time,
        start_time,
        end_time,
        k8s_env_name,
        '${e_dt_var}' as dt
    from modify_start_time;