set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set mapreduce.job.queuename=root.users.liuhao;

insert overwrite table cdmdws.dws_svo_user_qa_a_d
select
    user_qa_cate__cate_name_path,
    user_qa_cate__cate_id_path,
    user_qa_cate__cate_name,
    user_qa_cate__level,
--  dwd_cms_svo_user_qa_i_d
    user_qa__user_qa_id,
    user_qa__uuid,
    user_qa__sys_cate_id,
    user_qa__sv_cate_id,
    user_qa__user_qa_cate_id,
    user_qa__agent_id,
    user_qa__agent_secret,
    user_qa__title,
    user_qa__is_work,
    user_qa__is_work_name,
    user_qa__create_time,
    user_qa__event_time,
    user_qa__push_action,
    user_qa__push_action_name,
    user_qa__is_need_push,
    user_qa__is_del,
    user_qa__question,
    user_qa__answer,
    user_qa__emoji,
    user_qa__is_syn_status,
    user_qa__audit_name,
    user_qa__sv_msg,
    user_qa__news_id,
    user_qa__news_url,
    user_qa__payload,
    user_qa__source,
    user_qa__tags,
    user_qa__op_db,
    user_qa__k8s_env_name
from (
      select *,
             row_number()
                     over (partition by user_qa__user_qa_id order by user_qa__user_qa_id,user_qa__event_time DESC ) as rnk
      from cdmdwm.dwm_svo_user_qa_op_i_d
  ) as t1 where  t1.rnk=1

