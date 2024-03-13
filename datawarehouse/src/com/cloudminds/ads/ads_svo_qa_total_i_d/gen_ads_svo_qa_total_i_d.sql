set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;
set mapreduce.job.queuename=root.users.liuhao;
set hive.vectorized.execution.enabled=false;
with qa_total as (
    SELECT 
        'fqa' as qa_type,
        fqa_cate__cate_name_path as qa_cate_name_path,
        fqa__title as qa_title,
        fqa__is_work as qa_is_work,
        fqa__is_work_name as qa_is_work_name,
        fqa__is_del as qa_is_del,
        fqa__source as qa_source,
        fqa__sv_cate_id as qa_sv_cate_id,
        fqa__k8s_env_name as k8s_env_name,
        '' as sv_agent_id,
        count(distinct fqa__fqa_id) as total_cnt
    from cdmdws.dws_svo_fqa_a_d
    where 
        to_date(fqa__create_time) <= '${e_dt_var}'
    group by 
        fqa_cate__cate_name_path,
        fqa__title,
        fqa__is_work,
        fqa__is_work_name,
        fqa__is_del,
        fqa__source,
        fqa__sv_cate_id,
        fqa__k8s_env_name
    union all
    SELECT 
        'user_qa' as qa_type,
        user_qa_cate__cate_name_path as qa_cate_name_path,
        user_qa__title as qa_title,
        user_qa__is_work as qa_is_work,
        user_qa__is_work_name as qa_is_work_name,
        user_qa__is_del as qa_is_del,
        user_qa__source as qa_source,
        user_qa__sv_cate_id as qa_sv_cate_id,
        user_qa__k8s_env_name as k8s_env_name,
        user_qa__agent_id as sv_agent_id,
        count(distinct user_qa__user_qa_id) as total_cnt
    from cdmdws.dws_svo_user_qa_a_d
    where 
        to_date(user_qa__create_time) <= '${e_dt_var}'
    group by 
        user_qa_cate__cate_name_path,
        user_qa__title,
        user_qa__is_work,
        user_qa__is_work_name,
        user_qa__is_del,
        user_qa__source,
        user_qa__sv_cate_id,
        user_qa__k8s_env_name,
        user_qa__agent_id
)

insert overwrite table cdmads.ads_svo_qa_total_i_d partition(dt)
select 
    qa_type,
    nvl(qa_cate_name_path, '') as qa_cate_name_path,
    nvl(qa_title, '') as qa_title,
    nvl(qa_is_work, -99999998) as qa_is_work,
    nvl(qa_is_work_name, '') as qa_is_work_name,
    nvl(qa_is_del, -99999998) as qa_is_del,
    nvl(qa_source, '') as qa_source,
    nvl(qa_sv_cate_id, '') as qa_sv_cate_id,
    nvl(k8s_env_name, '') as k8s_env_name,
    total_cnt,
    sv_agent_id,
    '${e_dt_var}' as dt
from qa_total;