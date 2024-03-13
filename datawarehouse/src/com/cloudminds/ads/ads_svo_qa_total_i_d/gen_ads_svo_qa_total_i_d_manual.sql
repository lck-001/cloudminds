set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;
set mapreduce.job.queuename=root.users.liuhao;
set hive.vectorized.execution.enabled=false;
set hive.auto.convert.join=false;
set hive.ignore.mapjoin.hint=false;
with qa_dim as (
    select 
            qa_type,
            qa_cate_name_path,
            qa_title,
            qa_is_work,
            qa_is_work_name,
            qa_is_del,
            qa_source,
            qa_sv_cate_id,
            k8s_env_name,
            sv_agent_id,
            create_time
        from
        ( 
        select
            qa_type,
            qa_cate_name_path,
            qa_title,
            qa_is_work,
            qa_is_work_name,
            qa_is_del,
            qa_source,
            qa_sv_cate_id,
            k8s_env_name,
            sv_agent_id,
            to_date(create_time) as create_time
            row_number() over ( PARTITION by         
                qa_type,
                qa_cate_name_path,
                qa_title,
                qa_is_work,
                qa_is_work_name,
                qa_is_del,
                qa_source,
                qa_sv_cate_id,
                k8s_env_name,
                sv_agent_id
            ) as rnk
            from
            (
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
                    fqa__create_time as create_time
                from cdmdws.dws_svo_fqa_a_d
                where 
                    to_date(fqa__create_time) <= '${e_dt_var}'
                and
                    to_date(fqa__create_time) >= '${s_dt_var}'
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
                    user_qa__create_time as create_time
                from cdmdws.dws_svo_user_qa_a_d
                where 
                    to_date(user_qa__create_time) <= '${e_dt_var}'
                and
                    to_date(user_qa__create_time) >= '${s_dt_var}'
            ) m
        ) t
    where t.rnk = 1
),
history_cnt as (
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
        count(distinct fqa__fqa_id) as history_total_cnt
    from cdmdws.dws_svo_fqa_a_d
    where 
        to_date(fqa__create_time) < '${s_dt_var}'
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
        count(distinct user_qa__user_qa_id) as history_total_cnt
    from cdmdws.dws_svo_user_qa_a_d
    where 
        to_date(user_qa__create_time) < '${s_dt_var}'
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
),
qa_full as (
    select
        b.qa_type as qa_type,
        nvl(qa_cate_name_path, '') as qa_cate_name_path,
        nvl(qa_title, '') as qa_title,
        nvl(qa_is_work, -99999998) as qa_is_work,
        nvl(qa_is_work_name, '') as qa_is_work_name,
        nvl(qa_is_del, -99999998) as qa_is_del,
        nvl(qa_source, '') as qa_source,
        nvl(qa_sv_cate_id, '') as qa_sv_cate_id,
        nvl(k8s_env_name, '') as k8s_env_name,
        nvl(b.sv_agent_id, '') as sv_agent_id,
        a.dt as dt
    from 
    (
        select
            /*+MAPJOIN(a)*/
            qa_type,
            qa_cate_name_path,
            qa_title,
            qa_is_work,
            qa_is_work_name,
            qa_is_del,
            qa_source,
            qa_sv_cate_id,
            k8s_env_name,
            sv_agent_id
        from qa_dim
    ) b
    join
    (
        SELECT 
        date_add('${s_dt_var}', k.rnk - 1) as dt
        from  
        (
            SELECT 
            row_number() over() as rnk 
            from ( 
            SELECT  explode(split(rpad('0',DATEDIFF('${e_dt_var}','${s_dt_var}') ,',0'), ',')) as t ) 
        as m) as k
    ) a
    where a.create_time <= b.dt
),
qa_day_add as (
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
        to_date(fqa__create_time) as create_time,
        count(distinct fqa__fqa_id) as daliy_add_cnt
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
        fqa__k8s_env_name,
        to_date(fqa__create_time)
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
        to_date(user_qa__create_time) as create_time,
        count(distinct user_qa__user_qa_id) as daliy_add_cnt
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
        user_qa__agent_id,
        to_date(user_qa__create_time)
),
--补充时间维度数据
result_t as (
    select 
        nvl(a.qa_type, b.qa_type) as qa_type,
        nvl(a.qa_cate_name_path, b.qa_cate_name_path) as qa_cate_name_path,
        nvl(a.qa_title, b.qa_title) as qa_title,
        nvl(a.qa_is_work, b.qa_is_work) as qa_is_work,
        nvl(a.qa_is_work_name, b.qa_is_work_name) as qa_is_work_name,
        nvl(a.qa_is_del, b.qa_is_del) as qa_is_del,
        nvl(a.qa_source, b.qa_source) as qa_source,
        nvl(a.qa_sv_cate_id, b.qa_sv_cate_id) as qa_sv_cate_id,
        nvl(a.k8s_env_name, b.k8s_env_name) as k8s_env_name,
        nvl(a.sv_agent_id, b.sv_agent_id) as sv_agent_id,
        nvl(a.create_time, b.dt) as create_time,
        nvl(a.daliy_add_cnt, 0) as daliy_add_cnt
    from 
    (
        select 
            qa_type,
            qa_cate_name_path,
            qa_title,
            qa_is_work,
            qa_is_work_name,
            qa_is_del,
            qa_source,
            qa_sv_cate_id,
            k8s_env_name,
            sv_agent_id,
            create_time,
            daliy_add_cnt
        from qa_day_add
    ) as a
    right join
    (
        select 
            qa_type,
            qa_cate_name_path,
            qa_title,
            qa_is_work,
            qa_is_work_name,
            qa_is_del,
            qa_source,
            qa_sv_cate_id,
            k8s_env_name,
            sv_agent_id,
            dt
        from qa_full
    ) as b
    where a.create_time = b.dt
)

insert overwrite table cdmtmp.ads_svo_qa_total_i_d_tmp partition(dt)
select 
    qa_type,
    qa_cate_name_path,
    qa_title,
    qa_is_work,
    qa_is_work_name,
    qa_is_del,
    qa_source,
    qa_sv_cate_id,
    k8s_env_name,
    sum(daliy_add_cnt) over ( PARTITION by     
        qa_type,
        qa_cate_name_path,
        qa_title,
        qa_is_work,
        qa_is_work_name,
        qa_is_del,
        qa_source,
        qa_sv_cate_id,
        k8s_env_name,
        sv_agent_id
        order by create_time asc
    ) as total_cnt,
    sv_agent_id,
    create_time as dt
from result_t
