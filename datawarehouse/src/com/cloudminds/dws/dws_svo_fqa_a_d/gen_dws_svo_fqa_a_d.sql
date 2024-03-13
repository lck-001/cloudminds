set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set mapreduce.job.queuename=root.users.liuhao;
set spark.executor.memory = 2G;

insert overwrite table cdmdws.dws_svo_fqa_a_d
select
    -- dim_svo_fqa_cate_sh_d
    fqa_cate__cate_name_path,
    fqa_cate__cate_id_path,
    fqa_cate__cate_name,
    fqa_cate__level,
--  dwd_cms_svo_fqa_i_d
    fqa__fqa_id,
    fqa__fqa_cate_id,
    fqa__title,
    fqa__is_work,
    fqa__is_work_name,
    fqa__create_time,
    fqa__event_time,
    fqa__push_action,
    fqa__push_action_name,
    fqa__is_need_push,
    fqa__is_del,
    fqa__question,
    fqa__answer,
    fqa__emoji,
    fqa__last_seq_id,
    fqa__is_syn_status,
    fqa__audit_name,
    fqa__sv_msg,
    fqa__source,
    fqa__sys_cate_id,
    fqa__sv_cate_id,
    fqa__op_db,
    fqa__k8s_env_name

from (
         select *,
                row_number()
                        over (partition by fqa__fqa_id order by fqa__fqa_id,fqa__event_time DESC ) as rnk
         from cdmdwm.dwm_svo_fqa_op_i_d
     ) as t1 where  t1.rnk=1

