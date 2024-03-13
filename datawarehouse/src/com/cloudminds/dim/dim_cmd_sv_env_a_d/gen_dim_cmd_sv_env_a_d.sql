set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;
set mapreduce.job.queuename=root.users.liuhao;

insert overwrite table cdmdim.dim_cmd_sv_env_a_d
    select
       id as sv_env_id,
       nvl(env_name,'') as sv_env_name,
       nvl(tenant_code,'') as tenant_id,
       nvl(get_json_object(regexp_replace(env_value,'smartvoice.agent.baseUrl','smartvoiceagentbaseUrl'), '$.smartvoice.smartvoiceagentbaseUrl'),'') as sv_agent_base_url,
       nvl(get_json_object(regexp_replace(env_value,'smartvoice.agent.baseUrl.talk','smartvoiceagentbaseUrltalk'), '$.smartvoice.smartvoiceagentbaseUrltalk'),'') as sv_agent_base_url_talk,
       nvl(get_json_object(regexp_replace(env_value,'smartvoice.agent.baseUrl.confidence','smartvoiceagentbaseUrlconfidence'), '$.smartvoice.smartvoiceagentbaseUrlconfidence'),'') as sv_agent_base_url_confidence,
       nvl(get_json_object(regexp_replace(env_value,'smartvoice.agent.baseUrl.set.confidence','smartvoiceagentbaseUrlsetconfidence'), '$.smartvoice.smartvoiceagentbaseUrlsetconfidence'),'') as sv_agent_base_url_set_confidence,
       built_in as is_built_in,
       status as is_del,
       nvl(concat(create_time,'.000'), '') as create_time,
       nvl(concat(update_time,'.000'), '') as update_time,
       k8s_env_name
    from (select * from cdmods.ods_roc_db_c0002_t_environment_s_d where library_type='smartvoice' and dt = '${e_dt_var}' union all select * from cdmods.ods_roc_db_c0008_t_environment_s_d where library_type='smartvoice' and dt = '${e_dt_var}') a;