set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;
set spark.executor.memory = 4G;
set spark.driver.memory = 2G;
set mapreduce.job.queuename=root.users.liuhao;

with fqa_op as (
    select
        regexp_replace(faq_info, '\\\\r|\\\\n|\\t|\\001|\\\\n', '') as fqa
    from cdmods.ods_cms_db_c0017_faq_op_log_i_d
    where dt between date_sub('${s_dt_var}',2) and '${e_dt_var}'
),
fqa_op_old_expand as (
    select
        nvl(event_id,'') as event_id,
        nvl(fqa_id,'') as fqa_id,
        nvl(operator_id,'') as operator_id,
        nvl(operator,'') as operator,
        nvl(operation,'') as operation,
        cdmudf.arr2str(question_added,'###') as question_added,
        nvl(question_add_num,-99999998) as question_add_num,
        cdmudf.arr2str(answer_added,'###') as answer_added,
        nvl(answer_add_num,-99999998) as answer_add_num,
        nvl(fqa_id_old,'') as fqa_id_old,
        nvl(fqa_cate_id_old,'') as fqa_cate_id_old,
        nvl(fqa_cate_name_old,'') as fqa_cate_name_old,
        case when trim(lower(nvl(status_old,''))) = 'yes' then 1
             else 0
        end as status_old,
        case when trim(lower(nvl(status_old,''))) = 'yes' then '生效'
             else '失效'
        end as status_name_old,
        nvl(emoji_old,'') as emoji_old,
        nvl(tags_old,'') as tags_old,
        cdmudf.arr2str(question_sim_old,'###') as question_sim_old,
        cdmudf.arr2str(answer_sim_old,'###') as answer_sim_old,
        nvl(fqa_id_new,'') as fqa_id_new,
        nvl(fqa_cate_id_new,'') as fqa_cate_id_new,
        nvl(fqa_cate_name_new,'') as fqa_cate_name_new,
        case when trim(lower(nvl(status_new,''))) = 'yes' then 1
             else 0
        end as status_new,
        case when trim(lower(nvl(status_new,''))) = 'yes' then '生效'
             else '失效'
        end as status_name_new,
        nvl(emoji_new,'') as emoji_new,
        nvl(tags_new,'') as tags_new,
        cdmudf.arr2str(question_sim_new,'###') as question_sim_new,
        cdmudf.arr2str(answer_sim_new,'###') as answer_sim_new,
        nvl(file_path,'') as file_path,
        nvl(memo,'') as memo,
        nvl(`language`,'') as `language`,
        'bj-prod-232' as k8s_env_name,
        case when length(operation_time) = 10 then nvl(concat(from_unixtime( cast(operation_time as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
             when length(operation_time) = 19 then nvl(concat(operation_time,'.000'),'')
             when length(operation_time) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(operation_time,'T',' '),0,23),'')
             else nvl(concat(from_unixtime( cast(operation_time/1000 as bigint),'yyyy-MM-dd HH:mm:ss'),'.',substring(operation_time,11,3)),'')
        end as event_time,
        fqa as ext
    from fqa_op
    lateral view json_tuple(fqa,'_id', 'qa_id', 'operator_id','operator','operation','operation_time','q_added',
    'q_add_num','a_added','a_add_num','status_old','status_new','memo','language','file_path','data_old','data_new') a
    as event_id,fqa_id,operator_id,operator,operation,operation_time,question_added,question_add_num,answer_added,answer_add_num,status_old,status_new,memo,`language`,file_path,data_old,data_new
    lateral view json_tuple(data_old,'id','cate_id','cate_name','emoji','tags','qs','as') b as fqa_id_old,fqa_cate_id_old,fqa_cate_name_old,emoji_old,tags_old,question_sim_old,answer_sim_old
    lateral view json_tuple(data_new,'id','cate_id','cate_name','emoji','tags','qs','as') c as fqa_id_new,fqa_cate_id_new,fqa_cate_name_new,emoji_new,tags_new,question_sim_new,answer_sim_new
),
fqa_op_new_expand as (
    select
        nvl(event_id,'') as event_id,
        nvl(fqa_id,'') as fqa_id,
        nvl(operator_id,'') as operator_id,
        nvl(operator,'') as operator,
        nvl(operation,'') as operation,
        cdmudf.arr2str(question_added,'###') as question_added,
        nvl(question_add_num,-99999998) as question_add_num,
        cdmudf.arr2str(answer_added,'###') as answer_added,
        nvl(answer_add_num,-99999998) as answer_add_num,
        nvl(fqa_id_old,'') as fqa_id_old,
        nvl(fqa_cate_id_old,'') as fqa_cate_id_old,
        nvl(fqa_cate_name_old,'') as fqa_cate_name_old,
        case when trim(lower(nvl(status_old,''))) = 'yes' then 1
             else 0
        end as status_old,
        case when trim(lower(nvl(status_old,''))) = 'yes' then '生效'
             else '失效'
        end as status_name_old,
        nvl(emoji_old,'') as emoji_old,
        nvl(tags_old,'') as tags_old,
        cdmudf.arr2str(question_sim_old,'###') as question_sim_old,
        cdmudf.arr2str(answer_sim_old,'###') as answer_sim_old,
        sv_cate_id_old,
        nvl(fqa_id_new,'') as fqa_id_new,
        nvl(fqa_cate_id_new,'') as fqa_cate_id_new,
        nvl(fqa_cate_name_new,'') as fqa_cate_name_new,
        case when trim(lower(nvl(status_new,''))) = 'yes' then 1
             else 0
        end as status_new,
        case when trim(lower(nvl(status_new,''))) = 'yes' then '生效'
             else '失效'
        end as status_name_new,
        nvl(emoji_new,'') as emoji_new,
        nvl(tags_new,'') as tags_new,
        sv_cate_id_new,
        cdmudf.arr2str(question_sim_new,'###') as question_sim_new,
        cdmudf.arr2str(answer_sim_new,'###') as answer_sim_new,
        nvl(file_path,'') as file_path,
        nvl(memo,'') as memo,
        nvl(`language`,'') as `language`,
        'bj-prod-232' as k8s_env_name,
        case when length(operation_time) = 10 then nvl(concat(from_unixtime( cast(operation_time as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
             when length(operation_time) = 19 then nvl(concat(operation_time,'.000'),'')
             when length(operation_time) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(operation_time,'T',' '),0,23),'')
             else nvl(concat(from_unixtime( cast(operation_time/1000 as bigint),'yyyy-MM-dd HH:mm:ss'),'.',substring(operation_time,11,3)),'')
        end as event_time,
        fqa as ext
    from fqa_op
    lateral view json_tuple(fqa,'_id','qa_id','operator_id','operator','operation','operation_time','status_old','status_new','q_added','q_add_num','a_added','a_add_num','data_old','data_new','file_name','file_path','memo','language') a
    as event_id_json,qa_id_json,operator_id_json,operator,operation,operation_time_json,status_old,status_new,question_added,question_add_num,answer_added,answer_add_num,data_old_json,data_new_json,file_name,file_path,memo,`language`
    lateral view json_tuple(event_id_json,'$oid') b as event_id
    lateral view json_tuple(qa_id_json,'$numberLong') c as fqa_id
    lateral view json_tuple(operator_id_json,'$numberLong') d as operator_id
    lateral view json_tuple(operation_time_json,'$date') e as operation_time
    lateral view json_tuple(data_old_json,'id','qs','as','cate_id','cate_name','sv_cate_id','sv_cate_name','status','emoji','tags') f as fqa_id_old_json,question_sim_old,answer_sim_old,fqa_cate_id_old_json,fqa_cate_name_old,sv_cate_id_json_old,sv_cate_name_old,data_old_status,emoji_old,tags_old
    lateral view json_tuple(data_new_json,'id','qs','as','cate_id','cate_name','sv_cate_id','sv_cate_name','status','emoji','tags') g as fqa_id_new_json,question_sim_new,answer_sim_new,fqa_cate_id_new_json,fqa_cate_name_new,sv_cate_id_json_new,sv_cate_name_new,data_new_status,emoji_new,tags_new
    lateral view json_tuple(fqa_id_old_json,'$numberLong') h as fqa_id_old
    lateral view json_tuple(fqa_cate_id_old_json,'$numberLong') i as fqa_cate_id_old
    lateral view json_tuple(sv_cate_id_json_old,'$numberLong') j as sv_cate_id_old
    lateral view json_tuple(fqa_id_new_json,'$numberLong') k as fqa_id_new
    lateral view json_tuple(fqa_cate_id_new_json,'$numberLong') l as fqa_cate_id_new
    lateral view json_tuple(sv_cate_id_json_new,'$numberLong') m as sv_cate_id_new
),
merge_data as (
    select
        event_id,
        fqa_id,
        operator_id,
        operator,
        operation,
        question_added,
        question_add_num,
        answer_added,
        answer_add_num,
        fqa_id_old,
        fqa_cate_id_old,
        fqa_cate_name_old,
        status_old,
        status_name_old,
        emoji_old,
        tags_old,
        question_sim_old,
        answer_sim_old,
        fqa_id_new,
        fqa_cate_id_new,
        fqa_cate_name_new,
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
        ext,
        row_number() over (partition by event_id order by event_time asc ) as rnk
    from (
        select
        event_id,
        fqa_id,
        operator_id,
        operator,
        operation,
        question_added,
        question_add_num,
        answer_added,
        answer_add_num,
        fqa_id_old,
        fqa_cate_id_old,
        fqa_cate_name_old,
        status_old,
        status_name_old,
        emoji_old,
        tags_old,
        question_sim_old,
        answer_sim_old,
        fqa_id_new,
        fqa_cate_id_new,
        fqa_cate_name_new,
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
        ext
        from fqa_op_old_expand where date_sub('${s_dt_var}',2) <= to_date(event_time) and to_date(event_time) <= '${e_dt_var}'
        union all
        select
        event_id,
        fqa_id,
        operator_id,
        operator,
        operation,
        question_added,
        question_add_num,
        answer_added,
        answer_add_num,
        fqa_id_old,
        fqa_cate_id_old,
        fqa_cate_name_old,
        status_old,
        status_name_old,
        emoji_old,
        tags_old,
        question_sim_old,
        answer_sim_old,
        fqa_id_new,
        fqa_cate_id_new,
        fqa_cate_name_new,
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
        ext
        from fqa_op_new_expand where date_sub('${s_dt_var}',2) <= to_date(event_time) and to_date(event_time) <= '${e_dt_var}'
        ) t1
)

insert overwrite table cdmdwd.dwd_cms_svo_fqa_op_log_i_d partition(dt)
    select
        event_id,
        fqa_id,
        operator_id,
        operator,
        operation,
        question_added,
        question_add_num,
        answer_added,
        answer_add_num,
        fqa_id_old,
        fqa_cate_id_old,
        fqa_cate_name_old,
        status_old,
        status_name_old,
        emoji_old,
        tags_old,
        question_sim_old,
        answer_sim_old,
        fqa_id_new,
        fqa_cate_id_new,
        fqa_cate_name_new,
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
        ext,
        to_date(event_time) as dt
    from merge_data
    where rnk = 1;