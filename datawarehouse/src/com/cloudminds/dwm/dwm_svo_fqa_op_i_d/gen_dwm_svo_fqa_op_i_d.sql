--  modify by dave.liang at 2021-09-24

set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;
set mapreduce.job.queuename=root.users.liuhao;

insert overwrite table cdmdwm.dwm_svo_fqa_op_i_d partition(dt)
select
    --     dim_svo_user_qa_cate_sh_d
    ci.cate_name_path as fqa_cate__cate_name_path,
    ci.cate_id_path as fqa_cate__cate_id_path,
    ci.cate_name as fqa_cate__cate_name,
    ci.level as fqa_cate__level,
--     dwd_cms_svo_fqa_i_d
    ci.fqa_id as fqa__fqa_id,
    ci.fqa_cate_id as fqa__fqa_cate_id,
    ci.title as fqa__title,
    ci.is_work as fqa__is_work,
    ci.is_work_name as fqa__is_work_name,
    ci.create_time as fqa__create_time,
    ci.event_time as fqa__event_time,
    ci.push_action as fqa__push_action,
    ci.push_action_name as fqa__push_action_name,
    ci.is_need_push as fqa__is_need_push,
    ci.is_del as fqa__is_del,
    ci.question as fqa__question,
    ci.answer as fqa__answer,
    ci.emoji as fqa__emoji,
    ci.last_seq_id as fqa__last_seq_id,
    ci.is_syn_status as fqa__is_syn_status,
    ci.audit_name as fqa__audit_name,
    ci.sv_msg as fqa__sv_msg,
    ci.source as fqa__source,
    ci.sys_cate_id as fqa__sys_cate_id,
    ci.sv_cate_id as fqa__sv_cate_id,
    ci.op_db as fqa__op_db,
    ci.k8s_env_name as fqa__k8s_env_name,
    ci.dt as dt
from (
         select
             cate.cate_name_path,
             cate.cate_id_path,
             cate.cate_name,
             cate.level,
             cate.start_time,
             cate.end_time,
             --      dwd_cms_svo_fqa_i_d
             item.fqa_id,
             item.fqa_cate_id,
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
             item.last_seq_id,
             item.is_syn_status,
             item.audit_name,
             item.sv_msg,
             item.source,
             item.sys_cate_id,
             item.sv_cate_id,
             item.op_db,
             item.k8s_env_name,
             item.dt,
             case when cate.fqa_cate_id is null then false
                  else  true end as is_join
         from (select *
               from cdmdwd.dwd_cms_svo_fqa_i_d
               where dt >= '${s_dt_var}' and dt <= '${e_dt_var}'
              ) as item
                  left join (
             select *
             from cdmdim.dim_svo_fqa_cate_sh_d
             where dt = date_sub(current_date,1)
         ) as cate
                            ON item.fqa_cate_id = cate.fqa_cate_id
     ) as ci
where (ci.event_time between ci.start_time and  ci.end_time) or ci.is_join=false

