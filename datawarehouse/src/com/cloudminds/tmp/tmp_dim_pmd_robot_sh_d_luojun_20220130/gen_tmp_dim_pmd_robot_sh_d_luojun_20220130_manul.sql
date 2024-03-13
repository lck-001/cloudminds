set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;
set mapreduce.job.queuename=root.users.liuhao;

-- tmp 方案
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
         from cdmods.ods_crio_db_c0001_t_dict_s_d
         where dt = '${e_dt_var}' and `type` = 2
     ),
     crio_t_device as (
         select
             x1.robot_id,
             x1.robot_name,
             x1.robot_type_id,
             nvl(x2.robot_type_inner_name,'') as robot_type_inner_name,
             x1.robot_type_name as robot_type_name,
             x1.tenant_id,
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
             nvl(x1.robot_manufacturer_id,'') as robot_manufacturer_id,
             case when nvl(x3.robot_manufacturer_name,'') != '' then x3.robot_manufacturer_name
                  else nvl(x1.robot_manufacturer_name,'')
                 end as robot_manufacturer_name,
             x1.model,
             x1.software_version,
             x1.hardware_version,
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
                      nvl(t1.asset_code,'') as asset_code,
                      case when nvl(t1.asset_type,'') != '' then lpad(nvl(t1.asset_type,'001'),3,'0')
                           else '001'
                          end as asset_type,
                      case when trim(t1.asset_type) = '1' or trim(t1.asset_type) = '001' then '达闼固资'
                           when trim(t1.asset_type) = '2' or trim(t1.asset_type) = '002' then '达闼存资'
                           when trim(t1.asset_type) = '3' or trim(t1.asset_type) = '003' then '客户资产'
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
                      nvl(supplier_code,'') as robot_manufacturer_id,
                      nvl(supplier_code_name,'') as robot_manufacturer_name,
                      nvl(t1.device_model,'') as model,
                      nvl(t1.software_version,'') as software_version,
                      nvl(t1.hardware_version,'') as hardware_version,
                      nvl(t1.sku,'') as sku,
                      case when length(t1.quality_date) = 10 then nvl(concat(from_unixtime( cast(t1.quality_date as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
                           when length(t1.quality_date) = 19 then nvl(concat(from_utc_timestamp(t1.quality_date,'PRC'),'.000'),'')
                           when length(t1.quality_date) > 19 and length(t1.quality_date) < 23 then if(LENGTH(from_utc_timestamp(t1.quality_date,'PRC'))=19,nvl(concat(from_utc_timestamp(t1.quality_date,'PRC'),'.000'),''),nvl(rpad(from_utc_timestamp(t1.quality_date,'PRC'),23,'0'),''))
                           when length(t1.quality_date) >= 23 then nvl(from_utc_timestamp(SUBSTRING(REGEXP_REPLACE(t1.quality_date,'T',' '),0,23),'PRC'),'')
                          -- when length(t1.quality_date) = 19 then nvl(concat(t1.quality_date,'.000'),'')
                          -- when length(t1.quality_date) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(t1.quality_date,'T',' '),0,23),'')
                           else nvl(concat(from_unixtime( cast(t1.quality_date/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(t1.quality_date,11,3)),'')
                          end as quality_date,
                      case when length(t1.customer_quality_date) = 10 then nvl(concat(from_unixtime( cast(t1.customer_quality_date as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
                           when length(t1.customer_quality_date) = 19 then nvl(concat(from_utc_timestamp(t1.customer_quality_date,'PRC'),'.000'),'')
                           when length(t1.customer_quality_date) > 19 and length(t1.customer_quality_date) < 23 then if(LENGTH(from_utc_timestamp(t1.customer_quality_date,'PRC'))=19,nvl(concat(from_utc_timestamp(t1.customer_quality_date,'PRC'),'.000'),''),nvl(rpad(from_utc_timestamp(t1.customer_quality_date,'PRC'),23,'0'),''))
                           when length(t1.customer_quality_date) >= 23 then nvl(from_utc_timestamp(SUBSTRING(REGEXP_REPLACE(t1.customer_quality_date,'T',' '),0,23),'PRC'),'')
                          -- when length(t1.customer_quality_date) = 19 then nvl(concat(t1.customer_quality_date,'.000'),'')
                          -- when length(t1.customer_quality_date) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(t1.customer_quality_date,'T',' '),0,23),'')
                           else nvl(concat(from_unixtime( cast(t1.customer_quality_date/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(t1.customer_quality_date,11,3)),'')
                          end as customer_quality_date,
                      case when length(t1.product_date) = 10 then nvl(concat(from_unixtime( cast(t1.product_date as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
                           when length(t1.product_date) = 19 then nvl(concat(from_utc_timestamp(t1.product_date,'PRC'),'.000'),'')
                           when length(t1.product_date) > 19 and length(t1.product_date) < 23 then if(LENGTH(from_utc_timestamp(t1.product_date,'PRC'))=19,nvl(concat(from_utc_timestamp(t1.product_date,'PRC'),'.000'),''),nvl(rpad(from_utc_timestamp(t1.product_date,'PRC'),23,'0'),''))
                           when length(t1.product_date) >= 23 then nvl(from_utc_timestamp(SUBSTRING(REGEXP_REPLACE(t1.product_date,'T',' '),0,23),'PRC'),'')
                          -- when length(t1.product_date) = 19 then nvl(concat(t1.product_date,'.000'),'')
                          -- when length(t1.product_date) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(t1.product_date,'T',' '),0,23),'')
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
                      nvl(t1.environment,'') as environment,
                      case when length(t1.create_time) = 10 then nvl(concat(from_unixtime( cast(t1.create_time as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
                           when length(t1.create_time) = 19 then nvl(concat(from_utc_timestamp(t1.create_time,'PRC'),'.000'),'')
                           when length(t1.create_time) > 19 and length(t1.create_time) < 23 then if(LENGTH(from_utc_timestamp(t1.create_time,'PRC'))=19,nvl(concat(from_utc_timestamp(t1.create_time,'PRC'),'.000'),''),nvl(rpad(from_utc_timestamp(t1.create_time,'PRC'),23,'0'),''))
                           when length(t1.create_time) >= 23 then nvl(from_utc_timestamp(SUBSTRING(REGEXP_REPLACE(t1.create_time,'T',' '),0,23),'PRC'),'')
                          --  when length(t1.create_time) = 19 then nvl(concat(t1.create_time,'.000'),'')
                          -- when length(t1.create_time) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(t1.create_time,'T',' '),0,23),'')
                           else nvl(concat(from_unixtime( cast(t1.create_time/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(t1.create_time,11,3)),'')
                          end as create_time,
                      case when length(t1.update_time) = 10 then nvl(concat(from_unixtime( cast(t1.update_time as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
                           when length(t1.update_time) = 19 then nvl(concat(from_utc_timestamp(t1.update_time,'PRC'),'.000'),'')
                           when length(t1.update_time) > 19 and length(t1.update_time) < 23 then if(LENGTH(from_utc_timestamp(t1.update_time,'PRC'))=19,nvl(concat(from_utc_timestamp(t1.update_time,'PRC'),'.000'),''),nvl(rpad(from_utc_timestamp(t1.update_time,'PRC'),23,'0'),''))
                           when length(t1.update_time) >= 23 then nvl(from_utc_timestamp(SUBSTRING(REGEXP_REPLACE(t1.update_time,'T',' '),0,23),'PRC'),'')
                          -- when length(t1.update_time) = 19 then nvl(concat(t1.update_time,'.000'),'')
                          -- when length(t1.update_time) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(t1.update_time,'T',' '),0,23),'')
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
-- 初始化时注释掉    where substring(x1.update_time,0,10) >= '${s_dt_var}' and substring(x1.update_time,0,10) <= '${e_dt_var}'
     ),
     roc_t_robot as (
         select
             p1.robot_id,
             nvl(p1.robot_name,'') as robot_name,
             nvl(p1.robot_type_id,'') as robot_type_id,
             nvl(p2.robot_type_inner_name,'') as robot_type_inner_name,
             nvl(p2.robot_type_name,'') as robot_type_name,
             nvl(p1.tenant_id,'') as tenant_id,
             '' as asset_code,
             '001' as asset_type,
             '达闼固资' as asset_type_name,
             '' as product_type_code,
             '' as product_type_code_name,
             '' as product_id,
             '' as product_id_name,
             1 as asset_status,
             '在册' as asset_status_name,
             1 as status,
             '正常' as status_name,
             nvl(p1.robot_manufacturer_id,'') as robot_manufacturer_id,
             nvl(p3.robot_manufacturer_name,'') as robot_manufacturer_name,
             nvl(p1.model,'') as model,
             '' as software_version,
             '' as hardware_version,
             nvl(p1.sku,'') as sku,
             '' as quality_date,
             '' as customer_quality_date,
             '' as product_date,
             0 as roc_delivery_status,
             '已交付' as roc_delivery_status_name,
             1 as is_special_asset,
             '' as serial_number,
             1 as operating_status,
             '空闲' as operating_status_name,
             1 as running_status,
             '良好' as running_status_name,
             k8s_env_name as environment,
             k8s_env_name,
             create_time,
             update_time
         from (
                  select
                      robot_id,
                      robot_name,
                      robot_type_id,
                      tenant_id,
                      robot_manufacturer_id,
                      model,
                      sku,
                      create_time,
                      update_time,
                      k8s_env_name
                  from (
                           SELECT
                               t2.robot_id,
                               t2.robot_name,
                               t2.robot_type_id,
                               t2.tenant_id,
                               t2.robot_manufacturer_id,
                               t2.model,
                               t2.sku,
                               t2.create_time,
                               t2.update_time,
                               t2.k8s_env_name,
                               row_number() OVER (PARTITION BY t2.robot_id,t2.k8s_env_name ORDER BY t2.update_time DESC) as rnk
                           FROM cdmods.ods_crio_db_c0001_t_device_i_d t1
                                    right join (
                               select
                                   robot_code as robot_id,
                                   robot_name as robot_name,
                                   cast(nvl(robot_type,'') as string) as robot_type_id,
                                   tenant_code as tenant_id,
                                   manufacturer as robot_manufacturer_id,
                                   model as model,
                                   sku as sku,
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
                               from cdmods.ods_roc_db_c0002_t_robot_i_d
                               union all
                               select
                                   robot_code as robot_id,
                                   robot_name as robot_name,
                                   cast(nvl(robot_type,'') as string) as robot_type_id,
                                   tenant_code as tenant_id,
                                   manufacturer as robot_manufacturer_id,
                                   model as model,
                                   sku as sku,
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
                               from cdmods.ods_roc_db_c0008_t_robot_i_d
                           ) t2 on t1.device_code = t2.robot_id
                           where t1.device_code is null and t2.robot_id is not null
                       ) x1 where x1.rnk = 1
              ) p1
                  left join roc_t_dict p2 on p1.robot_type_id = p2.robot_type_id
                  left join crio_t_dict p3 on p1.robot_manufacturer_id = p3.robot_manufacturer_id
     ),
     remove_duplicate_robot as (
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
             asset_status,
             asset_status_name,
             status,
             status_name,
             robot_manufacturer_id,
             robot_manufacturer_name,
             model,
             software_version,
             hardware_version,
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
                      robot_manufacturer_id,
                      robot_manufacturer_name,
                      model,
                      software_version,
                      hardware_version,
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
                      create_time,
                      update_time,
                      k8s_env_name,
                      row_number() OVER (PARTITION BY robot_id,robot_name,robot_type_id,robot_type_inner_name,robot_type_name,tenant_id,asset_code,asset_type,product_type_code,product_type_code_name,product_id,product_id_name,asset_status,asset_status_name,status,status_name,robot_manufacturer_id,robot_manufacturer_name,model,software_version,hardware_version,sku,quality_date,customer_quality_date,product_date,roc_delivery_status,roc_delivery_status_name,is_special_asset,serial_number,operating_status,operating_status_name,running_status,running_status_name,environment,create_time,update_time,k8s_env_name ORDER BY update_time DESC) as rnk
                  from crio_t_device
                  union all
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
                      asset_status,
                      asset_status_name,
                      status,
                      status_name,
                      robot_manufacturer_id,
                      robot_manufacturer_name,
                      model,
                      software_version,
                      hardware_version,
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
                      create_time,
                      update_time,
                      k8s_env_name,
                      row_number() OVER (PARTITION BY robot_id,robot_name,robot_type_id,robot_type_inner_name,robot_type_name,tenant_id,asset_code,asset_type,product_type_code,product_type_code_name,product_id,product_id_name,asset_status,asset_status_name,status,status_name,robot_manufacturer_id,robot_manufacturer_name,model,software_version,hardware_version,sku,quality_date,customer_quality_date,product_date,roc_delivery_status,roc_delivery_status_name,is_special_asset,serial_number,operating_status,operating_status_name,running_status,running_status_name,environment,k8s_env_name ORDER BY create_time asc, update_time DESC) as rnk
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
             x1.robot_manufacturer_id,
             x1.robot_manufacturer_name,
             x1.model,
             x1.software_version,
             x1.hardware_version,
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
             case when x1.update_time != '' and x1.update_time is not null then x1.update_time
    else ''
end as start_time,
        CASE WHEN tmp_end_time IS NOT NULL AND tmp_end_time != '' THEN concat(from_unixtime(cast((cast(unix_timestamp(substring(tmp_end_time,0,19),'yyyy-MM-dd HH:mm:ss') AS bigint)*1000 + cast(substring(tmp_end_time,21,23) AS bigint) - 1)/1000 as bigint),'yyyy-MM-dd HH:mm:ss'),'.',SUBSTRING((cast(unix_timestamp(substring(tmp_end_time,0,19),'yyyy-MM-dd HH:mm:ss') AS bigint)*1000 + cast(substring(tmp_end_time,21,23) AS bigint) - 1),11,13))
             ELSE '9999-12-31 23:59:59.999'
END AS end_time,
       x1.k8s_env_name,
       x1.create_time,
       x1.update_time,
       row_number() OVER (PARTITION BY x1.robot_id ORDER BY create_time asc) as rnk,
       first_value(create_time,true) over (partition by robot_id order by create_time asc nulls last range between unbounded preceding and unbounded following) as tmp_create_time
   from (
        select
            t1.robot_id,
            t1.robot_name,
            t1.robot_type_id,
            t1.robot_type_inner_name,
            t1.robot_type_name,
            t1.tenant_id,
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
            t1.robot_manufacturer_id,
            t1.robot_manufacturer_name,
            t1.model,
            t1.software_version,
            t1.hardware_version,
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
                 robot_manufacturer_id,
                 robot_manufacturer_name,
                 model,
                 software_version,
                 hardware_version,
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
                 start_time,
                 end_time,
                 k8s_env_name,
                 create_time,
                 update_time
--  临时方案使用以下tmp表            from cdmdim.dim_pmd_robot_sh_d
                from cdmtmp.tmp_dim_pmd_robot_sh_d_luojun_20220130
             where dt = date_sub('${s_dt_var}',1)
             union all
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
                 asset_status,
                 asset_status_name,
                 status,
                 status_name,
                 robot_manufacturer_id,
                 robot_manufacturer_name,
                 model,
                 software_version,
                 hardware_version,
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
                 '' as start_time,
                 '' as end_time,
                 k8s_env_name,
                 create_time,
                 update_time
             from remove_duplicate_robot
         ) t1
   ) x1
),
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
        asset_status,
        asset_status_name,
        status,
        status_name,
        robot_manufacturer_id,
        robot_manufacturer_name,
        model,
        software_version,
        hardware_version,
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
        case when rnk = 1 then create_time
             else start_time
        end as start_time,
        end_time,
        k8s_env_name,
        create_time,
        update_time
    from merge_data
)
-- 临时方案使用以下tmp表 insert overwrite table cdmdim.dim_pmd_robot_sh_d partition(dt)
insert overwrite table cdmtmp.tmp_dim_pmd_robot_sh_d_luojun_20220130 partition(dt)
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
    asset_status,
    asset_status_name,
    status,
    status_name,
    robot_manufacturer_id,
    robot_manufacturer_name,
    model,
    software_version,
    hardware_version,
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
    start_time,
    end_time,
    k8s_env_name,
    create_time,
    update_time,
    '${e_dt_var}' as dt
from modify_start_time;
