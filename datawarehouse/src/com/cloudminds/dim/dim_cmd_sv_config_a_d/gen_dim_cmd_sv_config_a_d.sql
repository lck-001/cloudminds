set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;
set mapreduce.job.queuename=root.users.liuhao;

with tenantDataC0002 as (
  select t.robot_code,m.id,m.library_name,m.library_value,m.built_in,m.status,m.create_time,m.update_time,m.k8s_env_name,3 as type
  from cdmods.ods_roc_db_c0002_t_robot_s_d t
  left join (select b.*,a.tenant_code as tenant_id from cdmods.ods_roc_db_c0002_t_tenant_library_s_d a left join cdmods.ods_roc_db_c0002_t_library_s_d b on a.library_id=b.id and a.dt = '${e_dt_var}' and b.dt = '${e_dt_var}' where b.library_type='smartvoice') m
  on t.tenant_code=m.tenant_code and t.dt='${e_dt_var}' where m.tenant_code is not null
),
tenantDataC0008 as (
  select t.robot_code,m.id,m.library_name,m.library_value,m.built_in,m.status,m.create_time,m.update_time,m.k8s_env_name,3 as type
  from cdmods.ods_roc_db_c0008_t_robot_s_d t
  left join (select b.*,a.tenant_code as tenant_id from cdmods.ods_roc_db_c0008_t_tenant_library_s_d a left join cdmods.ods_roc_db_c0008_t_library_s_d b on a.library_id=b.id and a.dt = '${e_dt_var}' and b.dt = '${e_dt_var}' where b.library_type='smartvoice') m
  on t.tenant_code=m.tenant_code and t.dt='${e_dt_var}' where m.tenant_code is not null
),
robotDataC0002 as (
  select d.robot_code,c.id,c.library_name,c.library_value,c.built_in,c.status,c.create_time,c.update_time,c.k8s_env_name,1 as type
  from (select b.*,a.user_id from cdmods.ods_roc_db_c0002_t_user_library_s_d a left JOIN cdmods.ods_roc_db_c0002_t_library_s_d b on a.library_id=b.id and a.dt='${e_dt_var}' and b.dt='${e_dt_var}' where b.library_type='smartvoice') c
  left JOIN cdmods.ods_roc_db_c0002_t_user_rcu_robot_s_d d on c.user_id=d.user_id and d.`status`=1 and d.dt='${e_dt_var}'
  where d.robot_code is not null
),
robotDataC0008 as (
  select d.robot_code,c.id,c.library_name,c.library_value,c.built_in,c.status,c.create_time,c.update_time,c.k8s_env_name,1 as type
  from (select b.*,a.user_id from cdmods.ods_roc_db_c0008_t_user_library_s_d a left JOIN cdmods.ods_roc_db_c0008_t_library_s_d b on a.library_id=b.id and a.dt='${e_dt_var}' and b.dt='${e_dt_var}' where b.library_type='smartvoice') c
  left JOIN cdmods.ods_roc_db_c0008_t_user_rcu_robot_s_d d on c.user_id=d.user_id and d.`status`=1 and d.dt='${e_dt_var}'
  where d.robot_code is not null
),

tagDataC0002 as (
  select c.robot_code,e.id,e.library_name,e.library_value,e.built_in,e.status,e.create_time,e.update_time,e.k8s_env_name,2 as type
  from (select a.tag_id,b.robot_code,b.user_code,b.tenant_code from cdmods.ods_roc_db_c0002_t_tag_user_s_d a LEFT JOIN cdmods.ods_roc_db_c0002_t_user_rcu_robot_s_d b on a.user_id=b.user_id and b.`status`=1 and a.dt='${e_dt_var}' and b.dt='${e_dt_var}' where robot_code is not null) as c
  LEFT JOIN cdmods.ods_roc_db_c0002_t_tag_library_s_d d on c.tag_id=d.tag_id LEFT JOIN cdmods.ods_roc_db_c0002_t_library_s_d e on d.library_id=e.id and d.dt='${e_dt_var}' and e.dt='${e_dt_var}'
  where e.library_type='smartvoice'
),
allData as (
  select *,row_number() over (partition by robot_code,k8s_env_name order by type asc) as rank
  from (select * from tenantDataC0002 union all select * from tenantDataC0008 union all select * from robotDataC0002 union all select * from robotDataC0008 union all select * from tagDataC0002) b
)
insert overwrite table cdmdim.dim_cmd_sv_config_a_d
    select
       id as sv_config_id,
       nvl(library_name,'') as sv_config_name,
       robot_code as robot_id,
       nvl(get_json_object(regexp_replace(library_value,'smartvoice.agent.id','smartvoiceagentid'), '$.smartvoice.smartvoiceagentid'),'') as sv_agent_id,
       nvl(get_json_object(regexp_replace(library_value,'smartvoice.agent.name','smartvoiceagentname'), '$.smartvoice.smartvoiceagentname'),'') as sv_agent_name,
       built_in as is_built_in,
       status as is_del,
       nvl(concat(create_time,'.000'), '') as create_time,
       nvl(concat(update_time,'.000'), '') as update_time,
       k8s_env_name
    from allData where rank=1;