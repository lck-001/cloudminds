set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;
set spark.executor.memory = 4G;
set spark.locality.wait = 10;
set mapreduce.job.queuename=root.users.liuhao;

with  union_dim as (
    select
        t1.oid,
        t1.event_id,
        t1.project_code,
        t1.project_name,
        t1.event_type,
        t1.event_type_name,
        t1.description,
        t1.running_time,
        t1.finish_times,
        t1.mileage,
        t1.finish_point_tasks,
        t1.current_battery,
        t1.pub_time,
        t1.event_time,
        t1.k8s_env_name,
        --            customer dim
        nvl(t4.customer_id,'') as customer__customer_id,
        nvl(t4.customer_name,'') as customer__customer_name,
        nvl(t4.pid,'') as customer__pid,
        nvl(t4.`level`,'') as customer__level,
        nvl(t4.region,'') as customer__region,
        nvl(t4.address,'') as customer__address,
        nvl(t4.website,'') as customer__website,
        nvl(t4.industry_id,'') as customer__industry_id,
        nvl(t4.nature_code,'') as customer__nature_code,
        nvl(t4.nature_name,'') as customer__nature_name,
        nvl(t4.credit_code,'') as customer__credit_code,
        nvl(t4.credit_name,'') as customer__credit_name,
        nvl(t4.source_code,'') as customer__source_code,
        nvl(t4.source_name,'') as customer__source_name,
        nvl(t4.scale_code,'') as customer__scale_code,
        nvl(t4.scale_name,'') as customer__scale_name,
        nvl(t4.status_code,'') as customer__status_code,
        nvl(t4.status_name,'') as customer__status_name,
        nvl(t4.contact_code,'') as customer__contact_code,
        nvl(t4.contact_name,'') as customer__contact_name,
        nvl(t4.purchase_code,'') as customer__purchase_code,
        nvl(t4.purchase_name,'') as customer__purchase_name,
        nvl(t4.employment_code,'') as customer__employment_code,
        nvl(t4.employment_name,'') as customer__employment_name,
        nvl(t4.settlement_code,'') as customer__settlement_code,
        nvl(t4.settlement_name,'') as customer__settlement_name,
        nvl(t4.employer_num,-99999998) as customer__employer_num,
        nvl(t4.category_code,'') as customer__category_code,
        nvl(t4.category_name,'') as customer__category_name,
        nvl(t4.create_time,'') as customer__create_time,
--           tenant dim
        t1.tenant_id as tenant__tenant_id,
        nvl(t2.tenant_name,'') as tenant__tenant_name,
        nvl(t2.tenant_type,'') as tenant__tenant_type,
        nvl(t2.industry_id,'') as tenant__industry_id,
        nvl(t5.industry_name,'') as tenant__industry_name,
        nvl(t2.sub_sys,'') as tenant__sub_sys,
        nvl(t2.time_zone,'') as tenant__time_zone,
        nvl(t2.net_env_id,'') as tenant__net_env_id,
        nvl(t2.mcs_area,'') as tenant__mcs_area,
        nvl(t2.device_num,-99999998) as tenant__device_num,
        nvl(t2.region,'') as tenant__region,
        nvl(t2.address,'') as tenant__address,
        nvl(t2.contact,'') as tenant__contact,
        nvl(t2.phone,'') as tenant__phone,
        nvl(t2.telephone,'') as tenant__telephone,
        nvl(t2.seller,'') as tenant__seller,
        nvl(t2.email,'') as tenant__email,
        nvl(t2.boss_status,'-99999998') as tenant__boss_status,
        nvl(t2.boss_status_name,'未知') as tenant__boss_status_name,
        nvl(t2.roc_status,'-99999998') as tenant__roc_status,
        nvl(t2.roc_status_name,'未知') as tenant__roc_status_name,
        nvl(t2.charge_time,'') as tenant__charge_time,
        nvl(t2.expired_time,'') as tenant__expired_time,
        nvl(t2.logo,'') as tenant__logo,
        nvl(t2.description,'') as tenant__description,
        nvl(t2.brand,'') as tenant__brand,
        nvl(t2.is_need_vpn,0) as tenant__is_need_vpn,
        nvl(t2.create_time,'') as tenant__create_time,

--          robot dim
        t1.robot_id as robot__robot_id,
        nvl(t3.robot_name,'') as robot__robot_name,
        nvl(t3.robot_type_id,'')  as robot__robot_type_id,
        nvl(t3.robot_type_inner_name,'') as robot__robot_type_inner_name,
        nvl(t3.robot_type_name,'') as robot__robot_type_name,
        nvl(t3.asset_code,'') as robot__asset_code,
        nvl(t3.asset_type,'') as robot__asset_type,
        nvl(t3.product_type_code,'') as robot__product_type_code,
        nvl(t3.product_type_code_name,'') as robot__product_type_code_name,
        nvl(t3.product_id,'') as robot__product_id,
        nvl(t3.product_id_name,'') as robot__product_id_name,
        nvl(t3.assets_status,-99999998) as robot__assets_status,
        nvl(t3.assets_status_name,'未知') as robot__assets_status_name,
        nvl(t3.status,-99999998) as robot__status,
        nvl(t3.status_name,'未知') as robot__status_name,
        nvl(t3.robot_manufacturer_id,'') as robot__robot_manufacturer_id,
        nvl(t3.robot_manufacturer_name,'') as robot__robot_manufacturer_name,
        nvl(t3.model,'') as robot__model,
        nvl(t3.os,'') as robot__os,
        nvl(t3.version_code,'') as robot__version_code,
        nvl(t3.software_version,'') as robot__software_version,
        nvl(t3.hardware_version,'') as robot__hardware_version,
        nvl(t3.head_url,'') as robot__head_url,
        nvl(t3.regist_time,'') as robot__regist_time,
        cast(nvl(t3.bind_status,-99999998) as int) as robot__bind_status,
        nvl(t3.bind_status_name,'未知') as robot__bind_status_name,
        cast(nvl(t3.is_hi_service,-99999998) as int) as robot__is_hi_service,
        cast(nvl(t3.is_privacy_enhance,-99999998) as int) as robot__is_privacy_enhance,
        nvl(t3.remind_policy_id,'') as robot__remind_policy_id,
        nvl(t3.sku,'') as robot__sku,
        nvl(t3.quality_date,'') as robot__quality_date,
        nvl(t3.customer_quality_date,'') as robot__customer_quality_date,
        nvl(t3.out_type_state,-99999998) as robot__out_type_state,
        nvl(t3.out_type_state_name,'未知') as robot__out_type_state_name,
        nvl(t3.product_date,'') as robot__product_date,
        nvl(t3.in_stock_date,'') as robot__in_stock_date,
        nvl(t3.out_stock_date,'') as robot__out_stock_date,
        nvl(t3.in_stock_staff_id,'') as robot__in_stock_staff_id,
        nvl(t3.in_stock_staff_name,'') as robot__in_stock_staff_name,
        nvl(t3.out_stock_staff_id,'') as robot__out_stock_staff_id,
        nvl(t3.out_stock_staff_name,'') as robot__out_stock_staff_name,
        nvl(t3.note,'') as robot__robot_note,
        nvl(t3.description,'') as robot__description,
        nvl(t3.create_time,'') as robot__create_time,
        --task dim
        t1.programme_id as task_task_id,
        nvl(t1.programme_name,'') as task_task_name,
        nvl(t6.map_id,'') as task_map_id,
        nvl(t6.map_name,'') as task_map_name,
        nvl(t6.task_type_id,-99999998) as task_task_type_id,
        nvl(t6.task_type_name,'') as task_task_type_name,
        nvl(t6.work_types,'') as task_work_types,
        nvl(t6.work_type_names,'') as task_work_type_names,
        nvl(t6.schedule_type,'') as task_schedule_type,
        nvl(t6.schedule_start_time,'') as task_schedule_start_time,
        nvl(t6.schedule_end_time,'') as task_schedule_end_time,
        nvl(t6.schedule_days,'') as task_schedule_days,
        nvl(t6.repeat_times,-99999998) as task_repeat_times,
        nvl(t6.repeat_type,-99999998) as task_repeat_type,
        nvl(t6.repeat_type_name,'') as task_repeat_type_name,
        nvl(t6.sub_task,'') as task_sub_task,
        nvl(t6.is_del,-99999998) as task_is_del,
        nvl(t6.create_time,'') as task_create_time,
        nvl(t6.update_time,'') as task_update_time
    from  cdmdwd.dwd_crio_tmd_patrol_pause_a_d t1
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
             left join cdmdim.dim_umd_industry_a_d t5 on t2.industry_id = t5.industry_id left join cdmdim.dim_tmd_task_sh_d t6 on t1.programme_id=t6.task_id
    where ( t2.start_time is  null  or t1.event_time between t2.start_time and t2.end_time) and
        ( t3.start_time is null  or t1.event_time between t3.start_time and t3.end_time) and
        ( t4.start_time is null  or t1.event_time between t4.start_time and t4.end_time) and
        ( t6.start_time is null  or t1.event_time between t6.start_time and t6.end_time)
)

insert overwrite table cdmdwm.dwm_tmd_patrol_pause_a_d
select * from union_dim