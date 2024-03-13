set hive.exec.dynamic.partition.mode = nonstrict;
set hive.exec.max.dynamic.partitions = 60000;
set mapreduce.job.queuename=root.users.liuhao;

with qa_db_op as (
    select
           dt,
           'common_sense_qa'        as qa_type,
           ''                       as agent_id,
           fqa_cate__cate_name_path as cate_name_path,
           fqa_cate__cate_id_path   as cate_id_path,
           fqa_cate__level          as level,
           fqa__op_db               as op_db,
           fqa__is_del              as is_del,
           count(1)                 as cnt

    from (
             select *,
                    row_number()
                            over (partition by dt,fqa__fqa_id order by fqa__fqa_id,fqa__event_time DESC ) as rnk
             from cdmdwm.dwm_svo_fqa_op_i_d
             where
                dt <= '2021-10-08'
         ) as t1
    where t1.rnk = 1
    group by dt,'common_sense_qa', '', fqa_cate__cate_name_path, fqa_cate__cate_id_path, fqa_cate__level, fqa__op_db, fqa__is_del
    union all
    select
           dt,
           'user_qa'                    as qa_type,
           user_qa__agent_id            as agent_id,
           user_qa_cate__cate_name_path as cate_name_path,
           user_qa_cate__cate_id_path   as cate_id_path,
           user_qa_cate__level          as level,
           user_qa__op_db               as op_db,
           user_qa__is_del              as is_del,
           count(1)                     as cnt
    from (
             select *,
                    row_number()
                            over (partition by dt,user_qa__user_qa_id order by user_qa__user_qa_id,user_qa__event_time DESC ) as rnk
             from cdmdwm.dwm_svo_user_qa_op_i_d
             where
                dt <= '2021-10-08'
         ) as t1
    where t1.rnk = 1
    group by dt,'user_qa', user_qa__agent_id, user_qa_cate__cate_name_path, user_qa_cate__cate_id_path,user_qa_cate__level, user_qa__op_db,
             user_qa__is_del
)

insert overwrite table cdmdws.dws_svo_qa_db_op_i_d  partition (dt)
select
    cate_name_path,
    cate_id_path,
    level,
    qa_type,
    agent_id,
    is_del,
    op_db,
    cnt,
    'day' cnt_cycle,
    'bj_prod_232' k8s_env_name,
     dt
from qa_db_op

