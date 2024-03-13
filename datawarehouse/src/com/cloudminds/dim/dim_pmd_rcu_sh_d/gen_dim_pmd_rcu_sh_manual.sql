set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;
set mapreduce.job.queuename=root.users.liuhao;

-- boss 中的rcu
with crio_t_rcu as (
    select
        rcu_id,
        rcu_name,
        imei,
        did,
        did_credential_id,
        model,
        os,
        version_code,
        status,
        status_name,
        rcu_manufacturer_name,
        cpu,
        ram,
        rom_capacity,
        rom_available_capacity,
        sd_capacity,
        sd_available_capacity,
        camera,
        wifi_mac,
        bluetooth_mac,
        regist_time,
        activate_time,
        deactivate_time,
        description,
        k8s_env_name,
        create_time,
        update_time
    from (
        select
            nvl(rcu_code,'') as rcu_id,
            regexp_replace(nvl(trim(rcu_name),''), '\r|\n|\t', '') as rcu_name,
            nvl(imei,'') as imei,
            nvl(did,'') as did,
            '' as did_credential_id,
            nvl(model,'') as model,
            nvl(os,'') as os,
            nvl(version_code,'') as version_code,
            case when status = 1 then 0
                 when status = 9 then -1
                 else -99999998
                end as status,
            case when status = 1 then '正常'
                 when status = 9 then '删除'
                 else '未知'
                end as status_name,
            nvl(manufacturer,'') as rcu_manufacturer_name,
            '' as cpu,
            '' as ram,
            '' as rom_capacity,
            '' as rom_available_capacity,
            '' as sd_capacity,
            '' as sd_available_capacity,
            '' as camera,
            '' as wifi_mac,
            '' as bluetooth_mac,
            '' as regist_time,
            '' as activate_time,
            '' as deactivate_time,
            regexp_replace(nvl(trim(description),''), '\r|\n|\t', '') as description,
            nvl(k8s_env_name,'') as k8s_env_name,
            case when length(create_time) = 10 then nvl(concat(from_unixtime( cast(create_time as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
                 when length(create_time) = 19 then nvl(concat(from_utc_timestamp(create_time,'PRC'),'.000'),'')
                 when length(create_time) >= 23 then nvl(from_utc_timestamp(SUBSTRING(REGEXP_REPLACE(create_time,'T',' '),0,23),'PRC'),'')
                 else nvl(concat(from_unixtime( cast(create_time/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(create_time,11,3)),'')
            end as create_time,
            case when length(update_time) = 10 then nvl(concat(from_unixtime( cast(update_time as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
                 when length(update_time) = 19 then nvl(concat(from_utc_timestamp(update_time,'PRC'),'.000'),'')
                 when length(update_time) >= 23 then nvl(from_utc_timestamp(SUBSTRING(REGEXP_REPLACE(update_time,'T',' '),0,23),'PRC'),'')
                 else nvl(concat(from_unixtime( cast(update_time/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(update_time,11,3)),'')
            end as update_time
        from cdmods.ods_crio_db_c0001_t_rcu_s_d where dt <= '${e_dt_var}'
    ) t where substring(t.update_time,0,10) <= '${e_dt_var}'
),
-- roc 提取
roc_t_rcu as (
    select
        rcu_id,
        rcu_name,
        imei,
        did,
        did_credential_id,
        model,
        os,
        version_code,
        status,
        status_name,
        rcu_manufacturer_name,
        cpu,
        ram,
        rom_capacity,
        rom_available_capacity,
        sd_capacity,
        sd_available_capacity,
        camera,
        wifi_mac,
        bluetooth_mac,
        regist_time,
        activate_time,
        deactivate_time,
        description,
        k8s_env_name,
        create_time,
        update_time
    from (
         select
             nvl(rcu_code,'') as rcu_id,
             regexp_replace(nvl(trim(rcu_name),''), '\r|\n|\t', '') as rcu_name,
             nvl(imei,'') as imei,
             nvl(did,'') as did,
             nvl(did_credential_id,'') as did_credential_id,
             nvl(model,'') as model,
             nvl(os,'') as os,
             nvl(version_code,'') as version_code,
             nvl(status,-99999998) as status,
             case when status = -1 then '删除'
                  when status = 0 then '正常'
                  when status = 1 then '在线'
                  else '未知'
                 end as status_name,
             nvl(manufacturer,'') as rcu_manufacturer_name,
             nvl(cpu,'') as cpu,
             nvl(ram,'') as ram,
             nvl(rom_capacity,'') as rom_capacity,
             nvl(rom_available_capacity,'') as rom_available_capacity,
             nvl(sd_capacity,'') as sd_capacity,
             nvl(sd_available_capacity,'') as sd_available_capacity,
             nvl(camera,'') as camera,
             nvl(wifi_mac,'') as wifi_mac,
             nvl(bluetooth_mac,'') as bluetooth_mac,
             nvl(regist_time,'') as regist_time,
             nvl(activate_time,'') as activate_time,
             nvl(deactivate_time,'') as deactivate_time,
             regexp_replace(nvl(trim(description),''), '\r|\n|\t', '') as description,
             nvl(k8s_env_name,'') as k8s_env_name,
             case when length(regist_time) = 10 then nvl(concat(from_unixtime( cast(regist_time as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
                  when length(regist_time) = 19 then nvl(concat(regist_time,'.000'),'')
                  when length(regist_time) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(regist_time,'T',' '),0,23),'')
                  else nvl(concat(from_unixtime( cast(regist_time/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(regist_time,11,3)),'')
                 end as create_time,
             case when length(update_time) = 10 then nvl(concat(from_unixtime( cast(update_time as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
                  when length(update_time) = 19 then nvl(concat(update_time,'.000'),'')
                  when length(update_time) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(update_time,'T',' '),0,23),'')
                  else nvl(concat(from_unixtime( cast(update_time/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(update_time,11,3)),'')
                 end as update_time
         from cdmods.ods_roc_db_c0002_t_rcu_s_d where dt <= '${e_dt_var}'
         union
         select
             nvl(rcu_code,'') as rcu_id,
             regexp_replace(nvl(trim(rcu_name),''), '\r|\n|\t', '') as rcu_name,
             nvl(imei,'') as imei,
             nvl(did,'') as did,
             nvl(did_credential_id,'') as did_credential_id,
             nvl(model,'') as model,
             nvl(os,'') as os,
             nvl(version_code,'') as version_code,
             nvl(status,-99999998) as status,
             case when status = -1 then '删除'
                  when status = 0 then '正常'
                  when status = 1 then '在线'
                  else '未知'
                 end as status_name,
             nvl(manufacturer,'') as rcu_manufacturer_name,
             nvl(cpu,'') as cpu,
             nvl(ram,'') as ram,
             nvl(rom_capacity,'') as rom_capacity,
             nvl(rom_available_capacity,'') as rom_available_capacity,
             nvl(sd_capacity,'') as sd_capacity,
             nvl(sd_available_capacity,'') as sd_available_capacity,
             nvl(camera,'') as camera,
             nvl(wifi_mac,'') as wifi_mac,
             nvl(bluetooth_mac,'') as bluetooth_mac,
             nvl(regist_time,'') as regist_time,
             nvl(activate_time,'') as activate_time,
             nvl(deactivate_time,'') as deactivate_time,
             regexp_replace(nvl(trim(description),''), '\r|\n|\t', '') as description,
             nvl(k8s_env_name,'') as k8s_env_name,
             case when length(regist_time) = 10 then nvl(concat(from_unixtime( cast(regist_time as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
                  when length(regist_time) = 19 then nvl(concat(regist_time,'.000'),'')
                  when length(regist_time) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(regist_time,'T',' '),0,23),'')
                  else nvl(concat(from_unixtime( cast(regist_time/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(regist_time,11,3)),'')
                 end as create_time,
             case when length(update_time) = 10 then nvl(concat(from_unixtime( cast(update_time as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
                  when length(update_time) = 19 then nvl(concat(update_time,'.000'),'')
                  when length(update_time) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(update_time,'T',' '),0,23),'')
                  else nvl(concat(from_unixtime( cast(update_time/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(update_time,11,3)),'')
                 end as update_time
         from cdmods.ods_roc_db_c0008_t_rcu_s_d where dt <= '${e_dt_var}'
     ) t where substring(t.update_time,0,10) <= '${e_dt_var}'
),
-- roc中,因roc 业务中使用了last_online_time 导致 update 变化，而其他属性未变化，若last_online_time属性无需参考，则update_time 取当前rcu_id中记录未变的最早记录的update_time
-- 若last_online_time属性需要，则保留当前逻辑
-- 此实现方案需要全局数据扫描，会随着时间递增导致执行变慢，使用方案二替换，参考倒数第二模块，两次排序
-- roc_t_rcu_update as (
--     select
--         rcu_id,
--         rcu_name,
--         imei,
--         did,
--         did_credential_id,
--         model,
--         os,
--         version_code,
--         status,
--         status_name,
--         rcu_manufacturer_name,
--         cpu,
--         ram,
--         rom_capacity,
--         rom_available_capacity,
--         sd_capacity,
--         sd_available_capacity,
--         camera,
--         wifi_mac,
--         bluetooth_mac,
--         regist_time,
--         activate_time,
--         deactivate_time,
--         description,
--         k8s_env_name,
--         create_time,
--         update_time
--     from (
--          select
--              rcu_id,
--              rcu_name,
--              imei,
--              did,
--              did_credential_id,
--              model,
--              os,
--              version_code,
--              status,
--              status_name,
--              rcu_manufacturer_name,
--              cpu,
--              ram,
--              rom_capacity,
--              rom_available_capacity,
--              sd_capacity,
--              sd_available_capacity,
--              camera,
--              wifi_mac,
--              bluetooth_mac,
--              regist_time,
--              activate_time,
--              deactivate_time,
--              description,
--              k8s_env_name,
--              create_time,
--              update_time,
--              first_value(update_time,true) over(partition by rcu_id,rcu_name,imei,did,did_credential_id,model,os,version_code,status,status_name,rcu_manufacturer_name,cpu,ram,rom_capacity,
--              rom_available_capacity,sd_capacity,sd_available_capacity,camera,wifi_mac,bluetooth_mac,regist_time,activate_time,deactivate_time,description,k8s_env_name,create_time order by update_time asc nulls last range between unbounded preceding and unbounded following) as tmp_update_time
--          from roc_t_rcu
--     ) t where substring(tmp_update_time,0,10) = '${e_dt_var}'
-- ),

-- join 不成功 保留作为新数据
-- join 成功 先补充维度信息，然后和dt = '${e_dt_var}' and end_time = '9999-12-31 23:59:59.999' 进行排序 lead
un_join_data as (
    select
        case when t1.rcu_id is not null and t2.rcu_id is null then nvl(t1.rcu_id,'')
             else nvl(t2.rcu_id,'')
        end as rcu_id,
        case when t1.rcu_id is not null and t2.rcu_id is null then nvl(t1.rcu_name,'')
             else nvl(t2.rcu_name,'')
        end as rcu_name,
        case when t1.rcu_id is not null and t2.rcu_id is null then nvl(t1.imei,'')
             else nvl(t2.imei,'')
        end as imei,
        case when t1.rcu_id is not null and t2.rcu_id is null then nvl(t1.did,'')
             else nvl(t2.did,'')
        end as did,
        case when t1.rcu_id is not null and t2.rcu_id is null then nvl(t1.did_credential_id,'')
             else nvl(t2.did_credential_id,'')
        end as did_credential_id,
        case when t1.rcu_id is not null and t2.rcu_id is null then nvl(t1.model,'')
             else nvl(t2.model,'')
        end as model,
        case when t1.rcu_id is not null and t2.rcu_id is null then nvl(t1.os,'')
             else nvl(t2.os,'')
        end as os,
        case when t1.rcu_id is not null and t2.rcu_id is null then nvl(t1.version_code,'')
             else nvl(t2.version_code,'')
        end as version_code,
        case when t1.rcu_id is not null and t2.rcu_id is null then nvl(t1.status,-99999998)
             else nvl(t2.status,-99999998)
        end as status,
        case when t1.rcu_id is not null and t2.rcu_id is null then nvl(t1.status_name,'未知')
             else nvl(t2.status_name,'未知')
        end as status_name,
        case when t1.rcu_id is not null and t2.rcu_id is null then nvl(t1.rcu_manufacturer_name,'')
             else nvl(t2.rcu_manufacturer_name,'')
        end as rcu_manufacturer_name,
        case when t1.rcu_id is not null and t2.rcu_id is null then nvl(t1.cpu,'')
             else nvl(t2.cpu,'')
        end as cpu,
        case when t1.rcu_id is not null and t2.rcu_id is null then nvl(t1.ram,'')
             else nvl(t2.ram,'')
        end as ram,
        case when t1.rcu_id is not null and t2.rcu_id is null then nvl(t1.rom_capacity,'')
             else nvl(t2.rom_capacity,'')
        end as rom_capacity,
        case when t1.rcu_id is not null and t2.rcu_id is null then nvl(t1.rom_available_capacity,'')
             else nvl(t2.rom_available_capacity,'')
        end as rom_available_capacity,
        case when t1.rcu_id is not null and t2.rcu_id is null then nvl(t1.sd_capacity,'')
             else nvl(t2.sd_capacity,'')
        end as sd_capacity,
        case when t1.rcu_id is not null and t2.rcu_id is null then nvl(t1.sd_available_capacity,'')
             else nvl(t2.sd_available_capacity,'')
        end as sd_available_capacity,
        case when t1.rcu_id is not null and t2.rcu_id is null then nvl(t1.camera,'')
             else nvl(t2.camera,'')
        end as camera,
        case when t1.rcu_id is not null and t2.rcu_id is null then nvl(t1.wifi_mac,'')
             else nvl(t2.wifi_mac,'')
        end as wifi_mac,
        case when t1.rcu_id is not null and t2.rcu_id is null then nvl(t1.bluetooth_mac,'')
             else nvl(t2.bluetooth_mac,'')
        end as bluetooth_mac,
        case when t1.rcu_id is not null and t2.rcu_id is null then nvl(t1.regist_time,'')
             else nvl(t2.regist_time,'')
        end as regist_time,
        case when t1.rcu_id is not null and t2.rcu_id is null then nvl(t1.activate_time,'')
             else nvl(t2.activate_time,'')
        end as activate_time,
        case when t1.rcu_id is not null and t2.rcu_id is null then nvl(t1.deactivate_time,'')
             else nvl(t2.deactivate_time,'')
        end as deactivate_time,
        case when t1.rcu_id is not null and t2.rcu_id is null then nvl(t1.description,'')
             else nvl(t2.description,'')
        end as description,
        case when t1.rcu_id is not null and t2.rcu_id is null then nvl(t1.k8s_env_name,'')
             else nvl(t2.k8s_env_name,'')
        end as k8s_env_name,
        case when t1.rcu_id is not null and t2.rcu_id is null then nvl(t1.update_time,'')
             else nvl(t2.update_time,'')
        end as start_time,
        '9999-12-31 23:59:59.999' as end_time,
        case when t1.rcu_id is not null and t2.rcu_id is null then nvl(t1.create_time,'')
             else nvl(t2.create_time,'')
        end as create_time,
        case when t1.rcu_id is not null and t2.rcu_id is null then nvl(t1.update_time,'')
             else nvl(t2.update_time,'')
        end as update_time
    from (
         select
             rcu_id,
             rcu_name,
             imei,
             did,
             did_credential_id,
             model,
             os,
             version_code,
             status,
             status_name,
             rcu_manufacturer_name,
             cpu,
             ram,
             rom_capacity,
             rom_available_capacity,
             sd_capacity,
             sd_available_capacity,
             camera,
             wifi_mac,
             bluetooth_mac,
             regist_time,
             activate_time,
             deactivate_time,
             description,
             k8s_env_name,
             create_time,
             update_time
         from crio_t_rcu
         union
         select
             rcu_id,
             rcu_name,
             imei,
             did,
             did_credential_id,
             model,
             os,
             version_code,
             status,
             status_name,
             rcu_manufacturer_name,
             cpu,
             ram,
             rom_capacity,
             rom_available_capacity,
             sd_capacity,
             sd_available_capacity,
             camera,
             wifi_mac,
             bluetooth_mac,
             regist_time,
             activate_time,
             deactivate_time,
             description,
             k8s_env_name,
             create_time,
             update_time
         from roc_t_rcu
    ) t1 left join (
           select
               rcu_id,
               rcu_name,
               imei,
               did,
               did_credential_id,
               model,
               os,
               version_code,
               status,
               status_name,
               rcu_manufacturer_name,
               cpu,
               ram,
               rom_capacity,
               rom_available_capacity,
               sd_capacity,
               sd_available_capacity,
               camera,
               wifi_mac,
               bluetooth_mac,
               regist_time,
               activate_time,
               deactivate_time,
               description,
               start_time,
               end_time,
               k8s_env_name,
               create_time,
               update_time
           from cdmdim.dim_pmd_rcu_sh_d where dt = date_sub('${e_dt_var}',1) and end_time = '9999-12-31 23:59:59.999'
       ) t2 on t1.rcu_id = t2.rcu_id
    where t2.rcu_id is null
),
join_data as (
    select
        rcu_id,
        rcu_name,
        imei,
        did,
        did_credential_id,
        model,
        os,
        version_code,
        status,
        status_name,
        rcu_manufacturer_name,
        cpu,
        ram,
        rom_capacity,
        rom_available_capacity,
        sd_capacity,
        sd_available_capacity,
        camera,
        wifi_mac,
        bluetooth_mac,
        regist_time,
        activate_time,
        deactivate_time,
        description,
        start_time,
        end_time,
        k8s_env_name,
        create_time,
        update_time
    from cdmdim.dim_pmd_rcu_sh_d where dt = date_sub('${e_dt_var}',1)
    union
    select
        coalesce(if(nvl(t2.rcu_id,'') = '',null,t2.rcu_id),if(nvl(t1.rcu_id,'') = '',null,t1.rcu_id),'') as rcu_id,
        coalesce(if(nvl(t2.rcu_name,'') = '',null,t2.rcu_name),if(nvl(t1.rcu_name,'') = '',null,t1.rcu_name),'') as rcu_name,
        coalesce(if(nvl(t2.imei,'') = '',null,t2.imei),if(nvl(t1.imei,'') = '',null,t1.imei),'') as imei,
        coalesce(if(nvl(t2.did,'') = '',null,t2.did),if(nvl(t1.did,'') = '',null,t1.did),'') as did,
        coalesce(if(nvl(t2.did_credential_id,'') = '',null,t2.did_credential_id),if(nvl(t1.did_credential_id,'') = '',null,t1.did_credential_id),'') as did_credential_id,
        coalesce(if(nvl(t2.model,'') = '',null,t2.model),if(nvl(t1.model,'') = '',null,t1.model),'') as model,
        coalesce(if(nvl(t2.os,'') = '',null,t2.os),if(nvl(t1.os,'') = '',null,t1.os),'') as os,
        coalesce(if(nvl(t2.version_code,'') = '',null,t2.version_code),if(nvl(t1.version_code,'') = '',null,t1.version_code),'') as version_code,
        coalesce(if(nvl(t2.status,-99999998) = -99999998,null,t2.status),if(nvl(t1.status,-99999998) = -99999998,null,t1.status),-99999998) as status,
        coalesce(if(nvl(t2.status_name,'未知') = '未知',null,t2.status_name),if(nvl(t1.status_name,'未知') = '未知',null,t1.status_name),'未知') as status_name,
        coalesce(if(nvl(t2.rcu_manufacturer_name,'') = '',null,t2.rcu_manufacturer_name),if(nvl(t1.rcu_manufacturer_name,'') = '',null,t1.rcu_manufacturer_name),'') as rcu_manufacturer_name,
        coalesce(if(nvl(t2.cpu,'') = '',null,t2.cpu),if(nvl(t1.cpu,'') = '',null,t1.cpu),'') as cpu,
        coalesce(if(nvl(t2.ram,'') = '',null,t2.ram),if(nvl(t1.ram,'') = '',null,t1.ram),'') as ram,
        coalesce(if(nvl(t2.rom_capacity,'') = '',null,t2.rom_capacity),if(nvl(t1.rom_capacity,'') = '',null,t1.rom_capacity),'') as rom_capacity,
        coalesce(if(nvl(t2.rom_available_capacity,'') = '',null,t2.rom_available_capacity),if(nvl(t1.rom_available_capacity,'') = '',null,t1.rom_available_capacity),'') as rom_available_capacity,
        coalesce(if(nvl(t2.sd_capacity,'') = '',null,t2.sd_capacity),if(nvl(t1.sd_capacity,'') = '',null,t1.sd_capacity),'') as sd_capacity,
        coalesce(if(nvl(t2.sd_available_capacity,'') = '',null,t2.sd_available_capacity),if(nvl(t1.sd_available_capacity,'') = '',null,t1.sd_available_capacity),'') as sd_available_capacity,
        coalesce(if(nvl(t2.camera,'') = '',null,t2.camera),if(nvl(t1.camera,'') = '',null,t1.camera),'') as camera,
        coalesce(if(nvl(t2.wifi_mac,'') = '',null,t2.wifi_mac),if(nvl(t1.wifi_mac,'') = '',null,t1.wifi_mac),'') as wifi_mac,
        coalesce(if(nvl(t2.bluetooth_mac,'') = '',null,t2.bluetooth_mac),if(nvl(t1.bluetooth_mac,'') = '',null,t1.bluetooth_mac),'') as bluetooth_mac,
        coalesce(if(nvl(t2.regist_time,'') = '',null,t2.regist_time),if(nvl(t1.regist_time,'') = '',null,t1.regist_time),'') as regist_time,
        coalesce(if(nvl(t2.activate_time,'') = '',null,t2.activate_time),if(nvl(t1.activate_time,'') = '',null,t1.activate_time),'') as activate_time,
        coalesce(if(nvl(t2.deactivate_time,'') = '',null,t2.deactivate_time),if(nvl(t1.deactivate_time,'') = '',null,t1.deactivate_time),'') as deactivate_time,
        coalesce(if(nvl(t2.description,'') = '',null,t2.description),if(nvl(t1.description,'') = '',null,t1.description),'') as description,
        coalesce(if(nvl(t2.update_time,'') = '',null,t2.update_time),if(nvl(t1.update_time,'') = '',null,t1.update_time),'') as start_time,
        '9999-12-31 23:59:59.999' as end_time,
        coalesce(if(nvl(t2.k8s_env_name,'') = '',null,t2.k8s_env_name),if(nvl(t1.k8s_env_name,'') = '',null,t1.k8s_env_name),'') as k8s_env_name,
        coalesce(if(nvl(t2.create_time,'') = '',null,t2.create_time),if(nvl(t1.create_time,'') = '',null,t1.create_time),'') as create_time,
        coalesce(if(nvl(t2.update_time,'') = '',null,t2.update_time),if(nvl(t1.update_time,'') = '',null,t1.update_time),'') as update_time
    from (
         select
             rcu_id,
             rcu_name,
             imei,
             did,
             did_credential_id,
             model,
             os,
             version_code,
             status,
             status_name,
             rcu_manufacturer_name,
             cpu,
             ram,
             rom_capacity,
             rom_available_capacity,
             sd_capacity,
             sd_available_capacity,
             camera,
             wifi_mac,
             bluetooth_mac,
             regist_time,
             activate_time,
             deactivate_time,
             description,
             start_time,
             end_time,
             k8s_env_name,
             create_time,
             update_time
         from cdmdim.dim_pmd_rcu_sh_d where dt = date_sub('${e_dt_var}',1) and end_time = '9999-12-31 23:59:59.999'
    ) t1 left join (
       select
           rcu_id,
           rcu_name,
           imei,
           did,
           did_credential_id,
           model,
           os,
           version_code,
           status,
           status_name,
           rcu_manufacturer_name,
           cpu,
           ram,
           rom_capacity,
           rom_available_capacity,
           sd_capacity,
           sd_available_capacity,
           camera,
           wifi_mac,
           bluetooth_mac,
           regist_time,
           activate_time,
           deactivate_time,
           description,
           k8s_env_name,
           create_time,
           update_time
       from crio_t_rcu
       union
       select
           rcu_id,
           rcu_name,
           imei,
           did,
           did_credential_id,
           model,
           os,
           version_code,
           status,
           status_name,
           rcu_manufacturer_name,
           cpu,
           ram,
           rom_capacity,
           rom_available_capacity,
           sd_capacity,
           sd_available_capacity,
           camera,
           wifi_mac,
           bluetooth_mac,
           regist_time,
           activate_time,
           deactivate_time,
           description,
           k8s_env_name,
           create_time,
           update_time
       from roc_t_rcu
    ) t2 on t1.rcu_id = t2.rcu_id
    where t1.rcu_id is not null and t2.rcu_id is not null
),
merge_data as (
    select
        rcu_id,
        rcu_name,
        imei,
        did,
        did_credential_id,
        model,
        os,
        version_code,
        status,
        status_name,
        rcu_manufacturer_name,
        cpu,
        ram,
        rom_capacity,
        rom_available_capacity,
        sd_capacity,
        sd_available_capacity,
        camera,
        wifi_mac,
        bluetooth_mac,
        regist_time,
        activate_time,
        deactivate_time,
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
             rcu_id,
             rcu_name,
             imei,
             did,
             did_credential_id,
             model,
             os,
             version_code,
             status,
             status_name,
             rcu_manufacturer_name,
             cpu,
             ram,
             rom_capacity,
             rom_available_capacity,
             sd_capacity,
             sd_available_capacity,
             camera,
             wifi_mac,
             bluetooth_mac,
             regist_time,
             activate_time,
             deactivate_time,
             description,
             start_time,
             end_time,
             k8s_env_name,
             create_time,
             update_time,
             lead(update_time,1,NULL) over(partition BY rcu_id ORDER BY update_time ASC) tmp_end_time
         from join_data
     ) t1
    union
    select
        rcu_id,
        rcu_name,
        imei,
        did,
        did_credential_id,
        model,
        os,
        version_code,
        status,
        status_name,
        rcu_manufacturer_name,
        cpu,
        ram,
        rom_capacity,
        rom_available_capacity,
        sd_capacity,
        sd_available_capacity,
        camera,
        wifi_mac,
        bluetooth_mac,
        regist_time,
        activate_time,
        deactivate_time,
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
             rcu_id,
             rcu_name,
             imei,
             did,
             did_credential_id,
             model,
             os,
             version_code,
             status,
             status_name,
             rcu_manufacturer_name,
             cpu,
             ram,
             rom_capacity,
             rom_available_capacity,
             sd_capacity,
             sd_available_capacity,
             camera,
             wifi_mac,
             bluetooth_mac,
             regist_time,
             activate_time,
             deactivate_time,
             description,
             start_time,
             end_time,
             k8s_env_name,
             create_time,
             update_time,
             lead(update_time,1,NULL) over(partition BY rcu_id ORDER BY update_time ASC) tmp_end_time
         from un_join_data
     ) t2
),
column_dim as (
    select
        rcu_id,
        collect_set(case when rcu_name is not null and rcu_name != '' then concat_ws('->',rcu_id,end_time,rcu_name) else null end ) as rcu_name_dim,
        collect_set(case when imei is not null and imei != '' then concat_ws('->',rcu_id,end_time,imei) else null end ) as imei_dim,
        collect_set(case when did is not null and did != '' then concat_ws('->',rcu_id,end_time,did) else null end ) as did_dim,
        collect_set(case when did_credential_id is not null and did_credential_id != '' then concat_ws('->',rcu_id,end_time,did_credential_id) else null end ) as did_credential_id_dim,
        collect_set(case when model is not null and model != '' then concat_ws('->',rcu_id,end_time,model) else null end ) as model_dim,
        collect_set(case when os is not null and os != '' then concat_ws('->',rcu_id,end_time,os) else null end ) as os_dim,
        collect_set(case when version_code is not null and version_code != '' then concat_ws('->',rcu_id,end_time,version_code) else null end ) as version_code_dim,
        collect_set(case when status is not null and status != -99999998 then concat_ws('->',rcu_id,end_time,cast(status as string)) else null end ) as status_dim,
        collect_set(case when rcu_manufacturer_name is not null and rcu_manufacturer_name != '' then concat_ws('->',rcu_id,end_time,rcu_manufacturer_name) else null end ) as rcu_manufacturer_dim,
        collect_set(case when cpu is not null and cpu != '' then concat_ws('->',rcu_id,end_time,cpu) else null end ) as cpu_dim,
        collect_set(case when ram is not null and ram != '' then concat_ws('->',rcu_id,end_time,ram) else null end ) as ram_dim,
        collect_set(case when rom_capacity is not null and rom_capacity != '' then concat_ws('->',rcu_id,end_time,rom_capacity) else null end ) as rom_capacity_dim,
        collect_set(case when rom_available_capacity is not null and rom_available_capacity != '' then concat_ws('->',rcu_id,end_time,rom_available_capacity) else null end ) as rom_available_capacity_dim,
        collect_set(case when sd_capacity is not null and sd_capacity != '' then concat_ws('->',rcu_id,end_time,sd_capacity) else null end ) as sd_capacity_dim,
        collect_set(case when sd_available_capacity is not null and sd_available_capacity != '' then concat_ws('->',rcu_id,end_time,sd_available_capacity) else null end ) as sd_available_capacity_dim,
        collect_set(case when camera is not null and camera != '' then concat_ws('->',rcu_id,end_time,camera) else null end ) as camera_dim,
        collect_set(case when wifi_mac is not null and wifi_mac != '' then concat_ws('->',rcu_id,end_time,wifi_mac) else null end ) as wifi_mac_dim,
        collect_set(case when bluetooth_mac is not null and bluetooth_mac != '' then concat_ws('->',rcu_id,end_time,bluetooth_mac) else null end ) as bluetooth_mac_dim,
        collect_set(case when regist_time is not null and regist_time != '' then concat_ws('->',rcu_id,end_time,regist_time) else null end ) as regist_time_dim,
        collect_set(case when activate_time is not null and activate_time != '' then concat_ws('->',rcu_id,end_time,activate_time) else null end ) as activate_time_dim,
--         collect_set(case when deactivate_time is not null and deactivate_time != '' then concat_ws('->',rcu_id,end_time,deactivate_time) else null end ) as deactivate_time_dim,  -- 取消激活,不具有状态延续性，需要注释掉这段代码
        collect_set(case when description is not null and description != '' then concat_ws('->',rcu_id,end_time,description) else null end ) as description_dim
    from merge_data
    group by rcu_id
),
dim_repair as (
    select
        x1.rcu_id,
        case when nvl(x1.rcu_name_dim,'') != '' then x1.rcu_name_dim
             else x1.rcu_name
            end as rcu_name,
        case when nvl(x1.imei_dim,'') != '' then x1.imei_dim
             else x1.imei
            end as imei,
        case when nvl(x1.did_dim,'') != '' then x1.did_dim
             else x1.did
            end as did,
        case when nvl(x1.did_credential_id_dim,'') != '' then x1.did_credential_id_dim
             else x1.did_credential_id
            end as did_credential_id,
        case when nvl(x1.model_dim,'') != '' then x1.model_dim
             else x1.model
            end as model,
        case when nvl(x1.os_dim,'') != '' then x1.os_dim
             else x1.os
            end as os,
        case when nvl(x1.version_code_dim,'') != '' then x1.version_code_dim
             else x1.version_code
            end as version_code,
        case when nvl(x1.status_dim,'') != '' then cast(x1.status_dim as int)
             else nvl(x1.status,-99999998)
            end as status,
        case when nvl(x1.status_dim,'') != '' and cast(x1.status_dim as int) = 0 then '正常'
             when nvl(x1.status_dim,'') != '' and cast(x1.status_dim as int) = 1 then '停用'
             when nvl(x1.status_dim,'') != '' and cast(x1.status_dim as int) = -1 then '删除'
             else '未知'
        end as status_name,
        case when nvl(x1.rcu_manufacturer_dim,'') != '' then x1.rcu_manufacturer_dim
             else x1.rcu_manufacturer_name
        end as rcu_manufacturer_name,
        case when nvl(x1.cpu_dim,'') != '' then x1.cpu_dim
             else x1.cpu
        end as cpu,
        case when nvl(x1.ram_dim,'') != '' then x1.ram_dim
             else x1.ram
        end as ram,
        case when nvl(x1.rom_capacity_dim,'') != '' then x1.rom_capacity_dim
             else x1.rom_capacity
        end as rom_capacity,
        case when nvl(x1.rom_available_capacity_dim,'') != '' then x1.rom_available_capacity_dim
             else x1.rom_available_capacity
        end as rom_available_capacity,
        case when nvl(x1.sd_capacity_dim,'') != '' then x1.sd_capacity_dim
             else x1.sd_capacity
        end as sd_capacity,
        case when nvl(x1.sd_available_capacity_dim,'') != '' then x1.sd_available_capacity_dim
             else x1.sd_available_capacity
        end as sd_available_capacity,
        case when nvl(x1.camera_dim,'') != '' then x1.camera_dim
             else x1.camera
        end as camera,
        case when nvl(x1.wifi_mac_dim,'') != '' then x1.wifi_mac_dim
             else x1.wifi_mac
        end as wifi_mac,
        case when nvl(x1.bluetooth_mac_dim,'') != '' then x1.bluetooth_mac_dim
             else x1.bluetooth_mac
        end as bluetooth_mac,
        case when nvl(x1.regist_time_dim,'') != '' then x1.regist_time_dim
             else x1.regist_time
        end as regist_time,
        case when nvl(x1.activate_time_dim,'') != '' then x1.activate_time_dim
             else x1.activate_time
        end as activate_time,
--          case when nvl(x1.deactivate_time_dim,'') != '' then x1.deactivate_time_dim
--               else x1.deactivate_time
--          end as deactivate_time,
        nvl(x1.deactivate_time,'') as deactivate_time,
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
            t1.rcu_id,
            t1.rcu_name,
            t1.imei,
            t1.did,
            t1.did_credential_id,
            t1.model,
            t1.os,
            t1.version_code,
            t1.status,
            t1.status_name,
            t1.rcu_manufacturer_name,
            t1.cpu,
            t1.ram,
            t1.rom_capacity,
            t1.rom_available_capacity,
            t1.sd_capacity,
            t1.sd_available_capacity,
            t1.camera,
            t1.wifi_mac,
            t1.bluetooth_mac,
            t1.regist_time,
            t1.activate_time,
            t1.deactivate_time,
            t1.description,
            t1.start_time,
            t1.end_time,
            t1.k8s_env_name,
            t1.create_time,
            t1.update_time,
            case when t2.rcu_id is not null and size(t2.rcu_name_dim) > 0 then cdmudf.dim(t1.end_time,t2.rcu_name_dim)
                 else ''
            end as rcu_name_dim,
            case when t2.rcu_id is not null and size(t2.imei_dim) > 0 then cdmudf.dim(t1.end_time,t2.imei_dim)
                 else ''
            end as imei_dim,
            case when t2.rcu_id is not null and size(t2.did_dim) > 0 then cdmudf.dim(t1.end_time,t2.did_dim)
                 else ''
            end as did_dim,
            case when t2.rcu_id is not null and size(t2.did_credential_id_dim) > 0 then cdmudf.dim(t1.end_time,t2.did_credential_id_dim)
                 else ''
            end as did_credential_id_dim,
            case when t2.rcu_id is not null and size(t2.model_dim) > 0 then cdmudf.dim(t1.end_time,t2.model_dim)
                 else ''
            end as model_dim,
            case when t2.rcu_id is not null and size(t2.os_dim) > 0 then cdmudf.dim(t1.end_time,t2.os_dim)
                 else ''
            end as os_dim,
            case when t2.rcu_id is not null and size(t2.version_code_dim) > 0 then cdmudf.dim(t1.end_time,t2.version_code_dim)
                 else ''
            end as version_code_dim,
            case when t2.rcu_id is not null and size(t2.status_dim) > 0 then cdmudf.dim(t1.end_time,t2.status_dim)
                 else ''
            end as status_dim,
            case when t2.rcu_id is not null and size(t2.rcu_manufacturer_dim) > 0 then cdmudf.dim(t1.end_time,t2.rcu_manufacturer_dim)
                 else ''
            end as rcu_manufacturer_dim,
            case when t2.rcu_id is not null and size(t2.cpu_dim) > 0 then cdmudf.dim(t1.end_time,t2.cpu_dim)
                 else ''
            end as cpu_dim,
            case when t2.rcu_id is not null and size(t2.ram_dim) > 0 then cdmudf.dim(t1.end_time,t2.ram_dim)
                 else ''
            end as ram_dim,
            case when t2.rcu_id is not null and size(t2.rom_capacity_dim) > 0 then cdmudf.dim(t1.end_time,t2.rom_capacity_dim)
                 else ''
            end as rom_capacity_dim,
            case when t2.rcu_id is not null and size(t2.rom_available_capacity_dim) > 0 then cdmudf.dim(t1.end_time,t2.rom_available_capacity_dim)
                 else ''
            end as rom_available_capacity_dim,
            case when t2.rcu_id is not null and size(t2.sd_capacity_dim) > 0 then cdmudf.dim(t1.end_time,t2.sd_capacity_dim)
                 else ''
            end as sd_capacity_dim,
            case when t2.rcu_id is not null and size(t2.sd_available_capacity_dim) > 0 then cdmudf.dim(t1.end_time,t2.sd_available_capacity_dim)
                 else ''
            end as sd_available_capacity_dim,
            case when t2.rcu_id is not null and size(t2.camera_dim) > 0 then cdmudf.dim(t1.end_time,t2.camera_dim)
                 else ''
            end as camera_dim,
            case when t2.rcu_id is not null and size(t2.wifi_mac_dim) > 0 then cdmudf.dim(t1.end_time,t2.wifi_mac_dim)
                 else ''
            end as wifi_mac_dim,
            case when t2.rcu_id is not null and size(t2.bluetooth_mac_dim) > 0 then cdmudf.dim(t1.end_time,t2.bluetooth_mac_dim)
                 else ''
            end as bluetooth_mac_dim,
            case when t2.rcu_id is not null and size(t2.regist_time_dim) > 0 then cdmudf.dim(t1.end_time,t2.regist_time_dim)
                 else ''
            end as regist_time_dim,
            case when t2.rcu_id is not null and size(t2.activate_time_dim) > 0 then cdmudf.dim(t1.end_time,t2.activate_time_dim)
                 else ''
            end as activate_time_dim,
--              case when t2.rcu_id is not null and size(t2.deactivate_time_dim) > 0 then cdmudf.dim(t1.end_time,t2.deactivate_time_dim)
--                   else ''
--              end as deactivate_time_dim,
            case when t2.rcu_id is not null and size(t2.description_dim) > 0 then cdmudf.dim(t1.end_time,t2.description_dim)
                 else ''
            end as description_dim
        from merge_data t1
    left join column_dim t2 on t1.rcu_id = t2.rcu_id
   ) x1
),
-- 使用两次排序方案，对数据去重保持状态延续
-- 先按照rcu_id分组,end_time升序排序
-- 然后按照rcu_id,关键属性md5分组,end_time升序排序
-- 分组后的数值做差,按照rcu_id,关键属性md5,差值分组,组内更新end_time时间,返回更新后的记录即可
state_continue as (
    select
        rcu_id,
        rcu_name,
        imei,
        did,
        did_credential_id,
        model,
        os,
        version_code,
        status,
        status_name,
        rcu_manufacturer_name,
        cpu,
        ram,
        rom_capacity,
        rom_available_capacity,
        sd_capacity,
        sd_available_capacity,
        camera,
        wifi_mac,
        bluetooth_mac,
        regist_time,
        activate_time,
        deactivate_time,
        description,
        min(start_time) as start_time,
        max(end_time) as end_time,
        k8s_env_name,
        create_time,
        min(update_time) as update_time,
        row_number() OVER (PARTITION BY rcu_id ORDER BY create_time, min(update_time) asc) as rnk
    from (
        select
            rcu_id,
            rcu_name,
            imei,
            did,
            did_credential_id,
            model,
            os,
            version_code,
            status,
            status_name,
            rcu_manufacturer_name,
            cpu,
            ram,
            rom_capacity,
            rom_available_capacity,
            sd_capacity,
            sd_available_capacity,
            camera,
            wifi_mac,
            bluetooth_mac,
            regist_time,
            activate_time,
            deactivate_time,
            description,
            start_time,
            end_time,
            k8s_env_name,
            create_time,
            update_time,
            row_number() over(partition by rcu_id order by end_time asc) as rnk1,
            row_number() over(partition by rcu_id,rcu_name,imei,did,did_credential_id,model,os,version_code,status,status_name,rcu_manufacturer_name,cpu,ram,rom_capacity,rom_available_capacity,sd_capacity,sd_available_capacity,camera,wifi_mac,bluetooth_mac,regist_time,activate_time,deactivate_time,description,k8s_env_name,create_time order by end_time asc) as rnk2
        from dim_repair
    ) t group by rcu_id,rcu_name,imei,did,did_credential_id,model,os,version_code,status,status_name,rcu_manufacturer_name,cpu,ram,rom_capacity,rom_available_capacity,sd_capacity,sd_available_capacity,camera,wifi_mac,bluetooth_mac,regist_time,activate_time,deactivate_time,description,k8s_env_name,create_time,rnk2-rnk1
),
modify_start_time as (
    select
        rcu_id,
        rcu_name,
        imei,
        did,
        did_credential_id,
        model,
        os,
        version_code,
        status,
        status_name,
        rcu_manufacturer_name,
        cpu,
        ram,
        rom_capacity,
        rom_available_capacity,
        sd_capacity,
        sd_available_capacity,
        camera,
        wifi_mac,
        bluetooth_mac,
        regist_time,
        activate_time,
        deactivate_time,
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
insert overwrite table cdmdim.dim_pmd_rcu_sh_d partition(dt)
    select
        rcu_id,
        rcu_name,
        imei,
        did,
        did_credential_id,
        model,
        os,
        version_code,
        status,
        status_name,
        rcu_manufacturer_name,
        cpu,
        ram,
        rom_capacity,
        rom_available_capacity,
        sd_capacity,
        sd_available_capacity,
        camera,
        wifi_mac,
        bluetooth_mac,
        regist_time,
        activate_time,
        deactivate_time,
        description,
        start_time,
        end_time,
        k8s_env_name,
        create_time,
        update_time,
        '${e_dt_var}' as dt
    FROM modify_start_time;