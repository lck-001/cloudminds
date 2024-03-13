--  modify by dave.liang at 2021-09-24

set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;
set mapreduce.job.queuename=root.users.liuhao;

insert overwrite table cdmdwm.dwm_svo_user_qa_op_i_d partition(dt)
select
    --     dim_svo_user_qa_cate_sh_d
    ci.cate_name_path as user_qa_cate__cate_name_path,
    ci.cate_id_path as user_qa_cate__cate_id_path,
    ci.cate_name as user_qa_cate__cate_name,
    ci.level as user_qa_cate__level,
--     dwd_cms_svo_user_qa_i_d
    ci.user_qa_id as user_qa__user_qa_id,
    ci.uuid as user_qa__uuid,
    ci.sys_cate_id as user_qa__sys_cate_id,
    ci.sv_cate_id as user_qa__sv_cate_id,
    ci.user_qa_cate_id as user_qa__user_qa_cate_id,
    ci.agent_id as user_qa__agent_id,
    ci.agent_secret as user_qa__agent_secret,
    ci.title as user_qa__title,
    ci.is_work as user_qa__is_work,
    ci.is_work_name as user_qa__is_work_name,
    ci.create_time as user_qa__create_time,
    ci.event_time as user_qa__event_time,
    ci.push_action as user_qa__push_action,
    ci.push_action_name as user_qa__push_action_name,
    ci.is_need_push as user_qa__is_need_push,
    ci.is_del as user_qa__is_del,
    ci.question as user_qa__question,
    ci.answer as user_qa__answer,
    ci.emoji as user_qa__emoji,
    ci.is_syn_status as user_qa__is_syn_status,
    ci.audit_name as user_qa__audit_name,
    ci.sv_msg as user_qa__sv_msg,
    ci.news_id as user_qa__news_id,
    ci.news_url as user_qa__news_url,
    ci.payload as user_qa__payload,
    ci.source as user_qa__source,
    ci.tags as user_qa__tags,
    ci.op_db as user_qa__op_db,
    ci.k8s_env_name as user_qa__k8s_env_name,
    ci.dt as dt
from (
         select cate.cate_name_path,
                cate.cate_id_path,
                cate.cate_name,
                cate.level,
                cate.start_time,
                cate.end_time,
                --      dwd_cms_svo_user_qa_i_d
                item.user_qa_id,
                item.uuid,
                item.sys_cate_id,
                item.sv_cate_id,
                item.user_qa_cate_id,
                item.agent_id,
                item.agent_secret,
                item.title,
                item.is_work,
                item.is_work_name,
                item.create_time,
                item.event_time,
                item.push_action,
                item.push_action_name,
                item.is_need_push,
                item.is_del,
                item.question,
                item.answer,
                item.emoji,
                item.is_syn_status,
                item.audit_name,
                item.sv_msg,
                item.news_id,
                item.news_url,
                item.payload,
                item.source,
                item.tags,
                item.op_db,
                item.k8s_env_name,
                item.dt,
                case when cate.user_qa_cate_id is null then false
                     else  true end as is_join
         from (select *
               from cdmdwd.dwd_cms_svo_user_qa_i_d
               where dt >= '${s_dt_var}' and dt <= '${e_dt_var}'
              ) as item
                  left join (
             select *
             from cdmdim.dim_svo_user_qa_cate_sh_d
             where dt = date_sub(current_date,1)
         ) as cate
                            ON item.user_qa_cate_id = cate.user_qa_cate_id
     ) as ci
where (ci.event_time between ci.start_time and  ci.end_time) or ci.is_join=false

