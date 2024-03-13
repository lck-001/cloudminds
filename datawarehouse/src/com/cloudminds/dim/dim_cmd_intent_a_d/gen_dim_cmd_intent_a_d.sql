SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.merge.mapfiles = true;
SET hive.merge.mapredfiles = true;
SET hive.exec.max.dynamic.partitions = 60000;
set mapreduce.job.queuename=root.users.liuhao;

INSERT OVERWRITE TABLE cdmdim.dim_cmd_intent_a_d
    select
        nvl(id,'') as intent_id,
        nvl(domainid,'') as domain_id,
        nvl(agentid,'') as agent_id,
        nvl(intentname,'') as intent_name,
        nvl(inputcontext,'') as input_context,
        nvl(outputcontext,'') as output_context,
        nvl(voicepad,'') as voicepad,
        regexp_replace(nvl(trim(prompt),''), '\r|\n|\t', '') as prompt,
        nvl(action,'') as `action`,
        nvl(dfintentid,'') as df_intent_id,
        case when luaenabled = 1 or luaenabled = '1' then 1
             else 0
        end as lua_enabled,
        case when reply = 1 or reply = '1' then 1
             else 0
        end as reply,
        case when nvl(k8s_env_name,'') != '' then k8s_env_name
             else 'bj-prod-232'
        end as k8s_env_name,
        concat(current_date,' 00:00:00.000') as create_time,
        concat(current_date,' 00:00:00.000') as update_time
    from cdmods.ods_sv_db_c0003_intent_s_d
    where dt = '${e_dt_var}';