--  modify by dave.liang at 2021-09-22

set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;
set mapreduce.job.queuename=root.users.liuhao;

with  monitor_event_temp as (
     select
         t1.event_id,
         t1.event_type_id,
         t1.event_type_name_cn,
         t1.event_type_name_en,
         t1.rcu_id,
         t1.device_id,
         t1.event_time,
         t1.event_level,
         t1.event_details,
         t1.ext_data,
         t1.event_pic_url,
         t1.robot_location_info,
         t1.create_time,
         t1.update_time,
         t1.read_flag,
         t1.read_flag_name,
         t1.remark,
         t1.msg_type_id,
         t1.k8s_env_name,
         nvl(t6.sv_agent_id,'') as sv_agent_id,
--           tenant dim
         t1.tenant_id,
         nvl(t2.tenant_name,'') as tenant_name,
         nvl(t2.tenant_type,'') as tenant_type,
         nvl(t2.industry_id,'') as industry_id,
         nvl(t2.sub_sys,'') as sub_sys,
         nvl(t2.time_zone,'') as time_zone,
         nvl(t2.net_env_id,'') as net_env_id,
         nvl(t2.mcs_area,'') as mcs_area,
         nvl(t2.device_num,-99999998) as device_num,
         nvl(t2.region,'') as tenant_region,
         nvl(t2.address,'') as tenant_address,
         nvl(t2.contact,'') as contact,
         nvl(t2.phone,'') as tenant_phone,
         nvl(t2.telephone,'') as telephone,
         nvl(t2.seller,'') as seller,
         nvl(t2.email,'') as tenant_email,
         nvl(t2.status,'-99999998') as tenant_status,
         nvl(t2.status_name,'未知') as tenant_status_name,
         nvl(t2.charge_time,'') as charge_time,
         nvl(t2.expired_time,'') as expired_time,
         nvl(t2.logo,'') as logo,
         nvl(t2.description,'') as tenant__description,
         nvl(t2.brand,'') as brand,
         nvl(t2.is_need_vpn,0) as is_need_vpn,
         nvl(t2.create_time,'') as tenant__create_time,
         nvl(t5.industry_name,'') as tenant__industry_name,
--          robot dim
         t1.robot_id,
         nvl(t3.robot_name,'') as robot_name,
         nvl(t3.robot_type_id,'')  as robot_type_id,
         nvl(t3.robot_type_inner_name,'') as robot_type_inner_name,
         nvl(t3.robot_type_name,'') as robot_type_name,
         nvl(t3.asset_code,'') as asset_code,
         nvl(t3.asset_type,'') as asset_type,
         nvl(t3.product_type_code,'') as product_type_code,
         nvl(t3.product_type_code_name,'') as product_type_code_name,
         nvl(t3.product_id,'') as product_id,
         nvl(t3.product_id_name,'') as product_id_name,
         nvl(t3.assets_status,-99999998) as assets_status,
         nvl(t3.assets_status_name,'未知') as assets_status_name,
         nvl(t3.status,-99999998) as robot_status,
         nvl(t3.status_name,'未知') as robot_status_name,
         nvl(t3.robot_manufacturer_id,'') as robot_manufacturer_id,
         nvl(t3.robot_manufacturer_name,'') as robot_manufacturer_name,
         nvl(t3.model,'') as robot_model,
         nvl(t3.os,'') as robot_os,
         nvl(t3.version_code,'') as robot_version_code,
         nvl(t3.software_version,'') as software_version,
         nvl(t3.hardware_version,'') as hardware_version,
         nvl(t3.head_url,'') as head_url,
         nvl(t3.regist_time,'') as robot_regist_time,
         cast(nvl(t3.bind_status,-99999998) as int) as bind_status,
         nvl(t3.bind_status_name,'未知') as bind_status_name,
         cast(nvl(t3.is_hi_service,-99999998) as int) as is_hi_service,
         cast(nvl(t3.is_privacy_enhance,-99999998) as int) as is_privacy_enhance,
         nvl(t3.remind_policy_id,'') as remind_policy_id,
         nvl(t3.sku,'') as sku,
         nvl(t3.quality_date,'') as quality_date,
         nvl(t3.customer_quality_date,'') as customer_quality_date,
         nvl(t3.out_type_state,-99999998) as out_type_state,
         nvl(t3.out_type_state_name,'未知') as out_type_state_name,
         nvl(t3.product_date,'') as product_date,
         nvl(t3.in_stock_date,'') as in_stock_date,
         nvl(t3.out_stock_date,'') as out_stock_date,
         nvl(t3.in_stock_staff_id,'') as in_stock_staff_id,
         nvl(t3.in_stock_staff_name,'') as in_stock_staff_name,
         nvl(t3.out_stock_staff_id,'') as out_stock_staff_id,
         nvl(t3.out_stock_staff_name,'') as out_stock_staff_name,
         nvl(t3.note,'') as robot_note,
         nvl(t3.description,'') as description,
         nvl(t3.create_time,'') as robot_create_time,
--            customer dim
         nvl(t4.customer_id,'') as customer_id,
         nvl(t4.customer_name,'') as customer_name,
         nvl(t4.pid,'') as pid,
         nvl(t4.`level`,'') as level,
         nvl(t4.region,'') as customer_region,
         nvl(t4.address,'') as customer_address,
         nvl(t4.website,'') as website,
         nvl(t4.nature_code,'') as nature_code,
         nvl(t4.nature_name,'') as nature_name,
         nvl(t4.credit_code,'') as credit_code,
         nvl(t4.credit_name,'') as credit_name,
         nvl(t4.source_code,'') as source_code,
         nvl(t4.source_name,'') as source_name,
         nvl(t4.scale_code,'') as scale_code,
         nvl(t4.scale_name,'') as scale_name,
         nvl(t4.status_code,'') as status_code,
         nvl(t4.status_name,'') as status_name,
         nvl(t4.contact_code,'') as contact_code,
         nvl(t4.contact_name,'') as contact_name,
         nvl(t4.purchase_code,'') as purchase_code,
         nvl(t4.purchase_name,'') as purchase_name,
         nvl(t4.employment_code,'') as employment_code,
         nvl(t4.employment_name,'') as employment_name,
         nvl(t4.settlement_code,'') as settlement_code,
         nvl(t4.settlement_name,'') as settlement_name,
         nvl(t4.employer_num,-99999998) as employer_num,
         nvl(t4.category_code,'') as category_code,
         nvl(t4.category_name,'') as category_name,
         nvl(t4.create_time,'') as customer_create_time,
         t1.dt
     from (
             select
                event_id,
                  type_id       as event_type_id,
                  event_name    as event_type_name_cn,
                  type_name     as event_type_name_en,
                  robot_id,
                  tenant_id,
                  rcu_id,
                  device_id,
                  event_time,
                  level         as event_level,
                  details       as event_details,
                  ext_data,
                  pic_url       as event_pic_url,
                  location_info as robot_location_info,
                  create_time,
                  update_time,
                  read_flag,
                  read_flag_name,
                  remark,
                  msg_type_id,
                  k8s_env_name,
                  dt
           from cdmdwd.dwd_crio_pmd_event_i_d
    where dt = '${e_dt_var}'
--            where dt = '2021-09-18'
          ) as t1
         left join (
             select
                 tenant_id,
                 tenant_name,
                 tenant_type,
                 customer_id,
                 industry_id,
                 sub_sys,
                 time_zone,
                 net_env_id,
                 mcs_area,
                 device_num,
                 region,
                 address,
                 contact,
                 phone,
                 telephone,
                 seller,
                 email,
                 boss_status as status,
                 boss_status_name as status_name,
                 charge_time,
                 expired_time,
                 logo,
                 description,
                 brand,
                 is_need_vpn,
                 start_time,
                 end_time,
                 create_time,
                 k8s_env_name
             from cdmdim.dim_umd_tenant_sh_d
                 where dt = date_sub(current_date,1)
--              where dt = '2021-09-18' and
         ) t2 on t1.tenant_id = t2.tenant_id
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
                 where dt = date_sub(current_date,1)
--              where dt = '2021-09-18' and
--                  t1.event_time between start_time and  end_time
         ) t3 on t1.robot_id = t3.robot_id
         left join (
             select
                 customer_id,
                 customer_name,
                 pid,
                 level,
                 region,
                 address,
                 website,
                 industry_id,
                 nature_code,
                 nature_name,
                 credit_code,
                 credit_name,
                 source_code,
                 source_name,
                 scale_code,
                 scale_name,
                 status_code,
                 status_name,
                 contact_code,
                 contact_name,
                 purchase_code,
                 purchase_name,
                 employment_code,
                 employment_name,
                 settlement_code,
                 settlement_name,
                 employer_num,
                 category_code,
                 category_name,
                 create_time,
                 update_time,
                 start_time,
                 end_time
             from cdmdim.dim_umd_customer_sh_d
                 where dt = date_sub(current_date,1)
--              where dt = '2021-09-18'
         ) t4 on t2.customer_id = t4.customer_id
        left join cdmdim.dim_umd_industry_a_d t5 on t2.industry_id = t5.industry_id
        left join cdmdim.dim_cmd_sv_config_a_d t6 on t1.robot_id = t6.robot_id and t2.k8s_env_name= t6.k8s_env_name
        where ( t2.start_time is  null  or t1.event_time between t2.start_time and t2.end_time) and
         ( t3.start_time is null  or t1.event_time between t3.start_time and t3.end_time) and
         ( t4.start_time is null  or t1.event_time between t4.start_time and t4.end_time)
 )

insert overwrite table cdmdwm.dwm_omd_robot_monitor_event_i_d partition(dt)
select * from monitor_event_temp
