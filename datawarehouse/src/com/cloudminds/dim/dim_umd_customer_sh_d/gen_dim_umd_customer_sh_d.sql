set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;
set mapreduce.job.queuename=root.users.liuhao;

with newCustomer as(
   select
       cast(id as string) as customer_id,
       nvl(name,'') as customer_name,
       nvl(father_id,'') as pid,
       cast(cdmudf.LevelInfoUDF("id","father_id",CONCAT("cdmods.ods_crio_db_c0001_t_customer_s_d where dt='",'${e_dt_var}',"'"),null,id)['level'] as int) as level,
       nvl(region_name,'') as region,
       nvl(address,'') as address,
       nvl(website,'') as website,
       nvl(industry_code,'') as industry_id,
       nvl(nature_code,'') as nature_code,
       nvl(nature_name,'') as nature_name,
       nvl(credit_code,'') as credit_code,
       nvl(credit_name,'') as credit_name,
       nvl(source_code,'') as source_code,
       nvl(source_name,'') as source_name,
       nvl(scale_code,'') as scale_code,
       nvl(scale_name,'') as scale_name,
       nvl(status_code,'') as status_code,
       nvl(status_name,'') as status_name,
       nvl(contact_code,'') as contact_code,
       nvl(contact_name,'') as contact_name,
       nvl(purchase_code,'') as purchase_code,
       nvl(purchase_name,'') as purchase_name,
       nvl(employment_code,'') as employment_code,
       nvl(employment_name,'') as employment_name,
       nvl(settlement_code,'') as settlement_code,
       nvl(settlement_name,'') as settlement_name,
       nvl(staff_num, -99999998) as employer_num,
       nvl(category_code,'') as category_code,
       nvl(category_name,'') as category_name,
       CONCAT(create_time,".000") as create_time,
       CONCAT(update_time,".000") as update_time,
       CONCAT(update_time,".000") as start_time,
       '' as end_time,
       'bj-prod-232' as k8s_env_name,
       '' as dt
   from cdmods.ods_crio_db_c0001_t_customer_s_d
   where dt = '${e_dt_var}'),

allCustomer as (
     select *,
         row_number() over
         (partition by
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
               update_time
         ) as rnk
	 from (select * from newCustomer union all select * from cdmdim.dim_umd_customer_sh_d where dt=date_sub('${e_dt_var}', 1)) as t),

finalCustomer as (
   select
     *,
     concat(lead(update_time,1,NULL) over(partition BY customer_id ORDER BY update_time ASC),".000") tmp_end_time
   from allCustomer where rnk=1)

insert overwrite table cdmdim.dim_umd_customer_sh_d partition(dt)
   select
     a.customer_id,
     a.customer_name,
     a.pid,
     a.level,
     a.region,
     a.address,
     a.website,
     a.industry_id,
     a.nature_code,
     a.nature_name,
     a.credit_code,
     a.credit_name,
     a.source_code,
     a.source_name,
     a.scale_code,
     a.scale_name,
     a.status_code,
     a.status_name,
     a.contact_code,
     a.contact_name,
     a.purchase_code,
     a.purchase_name,
     a.employment_code,
     a.employment_name,
     a.settlement_code,
     a.settlement_name,
     a.employer_num,
     a.category_code,
     a.category_name,
     a.create_time,
     a.update_time,
     a.start_time,
     CASE WHEN tmp_end_time IS NOT NULL AND tmp_end_time != '' THEN concat(from_unixtime(cast(substring(cast(unix_timestamp(substring(tmp_end_time,0,19) ,'yyyy-MM-dd HH:mm:ss') as bigint)*1000+cast(substring(tmp_end_time,21,23) as bigint) - 1,0,10) AS BIGINT),'yyyy-MM-dd HH:mm:ss'),'.',substring(cast(unix_timestamp(substring(tmp_end_time,0,19) ,'yyyy-MM-dd HH:mm:ss') as bigint)*1000+cast(substring(tmp_end_time,21,23) as bigint) - 1,11,13))
                    ELSE '9999-12-31 23:59:59.999'
                END AS end_time,
     'bj-prod-232' as k8s_env_name,
     '${e_dt_var}' as dt
   FROM finalCustomer a;