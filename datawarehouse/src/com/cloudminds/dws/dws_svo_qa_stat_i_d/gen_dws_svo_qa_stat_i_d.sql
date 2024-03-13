set hive.exec.dynamic.partition.mode = nonstrict;
set hive.exec.max.dynamic.partitions = 60000;
set mapreduce.job.queuename=root.prod;

with qa as (
    SELECT '${e_dt_var}'                                                          as dt,
           'common_sense_qa'                                                     as qa_type,
           ''                                                                    as agent_id,
           fqa_cate__cate_name_path                                             as cate_name_path,
           fqa_cate__cate_name                                                  as cate_name,
           fqa_cate__level                                                      as cate_level,
           sum(CASE WHEN fqa__is_del = 0 and fqa__is_work = 1 THEN 1 ELSE 0 end) as work_total,
           sum(CASE WHEN fqa__is_del = 0 THEN 1 ELSE 0 end)                      as total
    from cdmdws.dws_svo_fqa_a_d
    where fqa__is_del = 0
    GROUP BY '${e_dt_var}', 'common_sense_qa', '', fqa_cate__cate_name_path, fqa_cate__cate_name, fqa_cate__level
    UNION
    SELECT '${e_dt_var}'                                                                  as dt,
           'user_qa'                                                                     as qa_type,
           user_qa__agent_id                                                             as agent_id,
           user_qa_cate__cate_name_path                                                 as cate_name_path,
           user_qa_cate__cate_name                                                      as cate_name,
           user_qa_cate__level                                                          as cate_level,
           sum(CASE WHEN user_qa__is_del = 0 and user_qa__is_work = 1 THEN 1 ELSE 0 end) as work_total,
           sum(CASE WHEN user_qa__is_del = 0 THEN 1 ELSE 0 end)                          as total
    from cdmdws.dws_svo_user_qa_a_d
    where user_qa__is_del = 0
    GROUP BY '${e_dt_var}', 'user_qa', user_qa__agent_id, user_qa_cate__cate_name_path, user_qa_cate__cate_name, user_qa_cate__level
)

insert overwrite table cdmdws.dws_svo_qa_stat_i_d partition (dt)
    select
           qa_type,
           cate_name_path,
           cate_name,
           cate_level,
           agent_id,
           work_total,
           total,
           dt
    from qa

