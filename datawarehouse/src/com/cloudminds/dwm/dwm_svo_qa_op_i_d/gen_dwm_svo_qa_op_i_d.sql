set hive.exec.dynamic.partition.mode = nonstrict;
set hive.exec.max.dynamic.partitions = 60000;
set mapreduce.job.queuename=root.users.liuhao;

with user_qa_op as (
    select
        event_id,
        user_qa_id as qa_id,
        'user_qa' as qa_type,
        operator_id,
        operator,
        operation,
        agent_id,
        question_added,
        question_add_num,
        answer_added,
        answer_add_num,
        user_qa_id_old as qa_id_old,
        user_qa_cate_id_old as qa_cate_id_old,
        user_qa_cate_name_old as qa_cate_name_old,
        status_old,
        status_name_old,
        emoji_old,
        tags_old,
        question_sim_old,
        answer_sim_old,
        user_qa_id_new as qa_id_new,
        user_qa_cate_id_new as qa_cate_id_new,
        user_qa_cate_name_new as qa_cate_name_new,
        status_new,
        status_name_new,
        emoji_new,
        tags_new,
        question_sim_new,
        answer_sim_new,
        file_path,
        memo,
        `language`,
        k8s_env_name,
        event_time,
        dt
    from cdmdwd.dwd_cms_svo_user_qa_op_log_i_d where dt >= '${s_dt_var}' and dt <= '${e_dt_var}'
),
fqa_op as (
    select
        event_id,
        fqa_id as qa_id,
        'common_scene_qa' as qa_type,
        operator_id,
        operator,
        operation,
        '' as agent_id,
        question_added,
        question_add_num,
        answer_added,
        answer_add_num,
        fqa_id_old as qa_id_old,
        fqa_cate_id_old as qa_cate_id_old,
        fqa_cate_name_old as qa_cate_name_old,
        status_old,
        status_name_old,
        emoji_old,
        tags_old,
        question_sim_old,
        answer_sim_old,
        fqa_id_new as qa_id_new,
        fqa_cate_id_new as qa_cate_id_new,
        fqa_cate_name_new as qa_cate_name_new,
        status_new,
        status_name_new,
        emoji_new,
        tags_new,
        question_sim_new,
        answer_sim_new,
        file_path,
        memo,
        `language`,
        k8s_env_name,
        event_time,
        dt
    from cdmdwd.dwd_cms_svo_fqa_op_log_i_d where dt >= '${s_dt_var}' and dt <= '${e_dt_var}'
),
qa_cate as (
    select
        fqa_cate_id as qa_cate_id,
        cate_name as qa_cate_name,
        pid,
        level,
        is_work,
        is_work_name,
        cate_name_path,
        cate_id_path,
        audit_name,
        create_time,
        k8s_env_name,
        start_time,
        end_time,
        'common_scene_qa' as qa_type
    from cdmdim.dim_svo_fqa_cate_sh_d where dt = date_sub(current_date,1)
    union all
    select
        user_qa_cate_id as qa_cate_id,
        cate_name as qa_cate_name,
        pid,
        level,
        is_work,
        is_work_name,
        cate_name_path,
        cate_id_path,
        audit_name,
        create_time,
        k8s_env_name,
        start_time,
        end_time,
        'user_qa' as qa_type
    from cdmdim.dim_svo_user_qa_cate_sh_d where dt = date_sub(current_date,1)
),
union_qa as (
    select
        t1.event_id,
        t1.qa_id,
        t1.qa_type,
        t1.operator_id,
        t1.operator,
        t1.operation,
        t1.agent_id,
        t1.question_added,
        t1.question_add_num,
        t1.answer_added,
        t1.answer_add_num,
        t1.qa_id_old,
        t1.qa_cate_id_old,
        case when t2.qa_cate_id is not null and nvl(t2.qa_cate_name,'') != '' then t2.qa_cate_name
             else t1.qa_cate_name_old
        end as qa_cate_name_old,
        t1.status_old,
        t1.status_name_old,
        t1.emoji_old,
        t1.tags_old,
        case when t2.qa_cate_id is not null then t2.pid
             else ''
        end as pid_old,
        case when t2.qa_cate_id is not null then t2.level
             else -99999998
        end as level_old,
        case when t2.qa_cate_id is not null then t2.is_work
             else -99999998
        end as is_work_old,
        case when t2.qa_cate_id is not null then t2.is_work_name
             else '未知'
        end as is_work_name_old,
        case when t2.qa_cate_id is not null then t2.cate_id_path
             else ''
        end as cate_id_path_old,
        case when t2.qa_cate_id is not null then t2.cate_name_path
             else ''
        end as cate_name_path_old,
        case when t2.qa_cate_id is not null then t2.audit_name
             else ''
        end as audit_name_old,
        t1.question_sim_old,
        size(split(t1.question_sim_old,'###')) as question_length_old,
        t1.answer_sim_old,
        size(split(t1.answer_sim_old,'###')) as answer_length_old,
        t1.qa_id_new,
        t1.qa_cate_id_new,
        case when t3.qa_cate_id is not null and nvl(t3.qa_cate_name,'') != '' then t3.qa_cate_name
             else t1.qa_cate_name_old
        end as qa_cate_name_new,
        t1.status_new,
        t1.status_name_new,
        t1.emoji_new,
        t1.tags_new,
        case when t3.qa_cate_id is not null then t3.pid
             else ''
        end as pid_new,
        case when t3.qa_cate_id is not null then t3.level
             else -99999998
        end as level_new,
        case when t3.qa_cate_id is not null then t3.is_work
             else -99999998
        end as is_work_new,
        case when t3.qa_cate_id is not null then t3.is_work_name
             else '未知'
        end as is_work_name_new,
        case when t3.qa_cate_id is not null then t3.cate_id_path
             else ''
        end as cate_id_path_new,
        case when t3.qa_cate_id is not null then t3.cate_name_path
             else ''
        end as cate_name_path_new,
        case when t3.qa_cate_id is not null then t3.audit_name
             else ''
        end as audit_name_new,
        t1.question_sim_new,
        size(split(t1.question_sim_new,'###')) as question_length_new,
        t1.answer_sim_new,
        size(split(t1.answer_sim_new,'###')) as answer_length_new,
        case when abs(size(split(t1.question_sim_old,'###')) - size(split(t1.question_sim_new,'###'))) = abs(size(split(t1.answer_sim_old,'###')) - size(split(t1.answer_sim_new,'###'))) then 1
                  else abs(size(split(t1.question_sim_old,'###')) - size(split(t1.question_sim_new,'###'))) + abs(size(split(t1.answer_sim_old,'###')) - size(split(t1.answer_sim_new,'###')))
        end as work_cnt,
        t1.file_path,
        t1.memo,
        t1.`language`,
        t1.k8s_env_name,
        t1.event_time,
        t1.dt
    from (
         select
            event_id,
            qa_id,
            qa_type,
            operator_id,
            operator,
            operation,
            agent_id,
            question_added,
            question_add_num,
            answer_added,
            answer_add_num,
            qa_id_old,
            qa_cate_id_old,
            qa_cate_name_old,
            status_old,
            status_name_old,
            emoji_old,
            tags_old,
            question_sim_old,
            answer_sim_old,
            qa_id_new,
            qa_cate_id_new,
            qa_cate_name_new,
            status_new,
            status_name_new,
            emoji_new,
            tags_new,
            question_sim_new,
            answer_sim_new,
            file_path,
            memo,
            `language`,
            k8s_env_name,
            event_time,
            dt
         from user_qa_op
         union all
         select
            event_id,
            qa_id,
            qa_type,
            operator_id,
            operator,
            operation,
            agent_id,
            question_added,
            question_add_num,
            answer_added,
            answer_add_num,
            qa_id_old,
            qa_cate_id_old,
            qa_cate_name_old,
            status_old,
            status_name_old,
            emoji_old,
            tags_old,
            question_sim_old,
            answer_sim_old,
            qa_id_new,
            qa_cate_id_new,
            qa_cate_name_new,
            status_new,
            status_name_new,
            emoji_new,
            tags_new,
            question_sim_new,
            answer_sim_new,
            file_path,
            memo,
            `language`,
            k8s_env_name,
            event_time,
            dt
         from fqa_op
    ) t1
    left join qa_cate t2 on t1.qa_cate_id_old = t2.qa_cate_id and t1.qa_type = t2.qa_type and t1.k8s_env_name = t2.k8s_env_name
    left join qa_cate t3 on t1.qa_cate_id_new = t3.qa_cate_id and t1.qa_type = t3.qa_type and t1.k8s_env_name = t3.k8s_env_name
    where (t2.start_time is null or t1.event_time between t2.start_time and t2.end_time)
    and (t3.start_time is null or t1.event_time between t3.start_time and t3.end_time)
)

insert overwrite table cdmdwm.dwm_svo_qa_op_i_d partition(dt)
    select
        event_id,
        qa_id,
        qa_type,
        operator_id,
        operator,
        operation,
        agent_id,
        question_added,
        question_add_num,
        answer_added,
        answer_add_num,
        qa_id_old,
        qa_cate_id_old,
        qa_cate_name_old,
        status_old,
        status_name_old,
        emoji_old,
        tags_old,
        pid_old,
        level_old,
        is_work_old,
        is_work_name_old,
        cate_id_path_old,
        cate_name_path_old,
        audit_name_old,
        question_sim_old,
        question_length_old,
        answer_sim_old,
        answer_length_old,
        qa_id_new,
        qa_cate_id_new,
        qa_cate_name_new,
        status_new,
        status_name_new,
        emoji_new,
        tags_new,
        pid_new,
        level_new,
        is_work_new,
        is_work_name_new,
        cate_id_path_new,
        cate_name_path_new,
        audit_name_new,
        question_sim_new,
        question_length_new,
        answer_sim_new,
        answer_length_new,
        work_cnt,
        file_path,
        memo,
        `language`,
        k8s_env_name,
        event_time,
        dt
    from union_qa;