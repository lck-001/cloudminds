set hive.exec.dynamic.partition.mode = nonstrict;
set hive.exec.max.dynamic.partitions = 60000;
set mapreduce.job.queuename=root.users.liuhao;

with qa_op_tmp as (
    select
        qa_id,
        qa_type,
        operator_id,
        operator,
        operation,
        agent_id,
        qa_id_old,
        qa_cate_id_old,
        qa_cate_name_old,
        emoji_old,
        tags_old,
        pid_old,
        level_old,
        cate_id_path_old,
        cate_name_path_old,
        qa_id_new,
        qa_cate_id_new,
        qa_cate_name_new,
        emoji_new,
        tags_new,
        pid_new,
        level_new,
        cate_id_path_new,
        cate_name_path_new,
        sum(work_cnt) as work_cnt_d,
        file_path,
        memo,
        `language`,
        dt
    from cdmdwm.dwm_svo_qa_op_i_d
    where dt >= '${s_dt_var}' and dt <= '${e_dt_var}'
    group by qa_id,qa_type,operator_id,operator,operation,agent_id,qa_id_old,qa_cate_id_old,qa_cate_name_old,emoji_old,tags_old,pid_old,level_old,cate_id_path_old,cate_name_path_old,qa_id_new,qa_cate_id_new,qa_cate_name_new,emoji_new,tags_new,pid_new,level_new,cate_id_path_new,cate_name_path_new,file_path,memo,`language`,dt
)
insert overwrite table cdmdws.dws_svo_qa_op_i_d partition(dt)
    select
        qa_id,
        qa_type,
        operator_id,
        operator,
        operation,
        agent_id,
        qa_id_old,
        qa_cate_id_old,
        qa_cate_name_old,
        emoji_old,
        tags_old,
        pid_old,
        level_old,
        cate_id_path_old,
        cate_name_path_old,
        qa_id_new,
        qa_cate_id_new,
        qa_cate_name_new,
        emoji_new,
        tags_new,
        pid_new,
        level_new,
        cate_id_path_new,
        cate_name_path_new,
        work_cnt_d,
        file_path,
        memo,
        `language`,
        dt
    from qa_op_tmp;