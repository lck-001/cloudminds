set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;
set mapreduce.job.queuename=root.users.liuhao;

with merge_asr as (
    select
        t1.question_id,
        case when nvl(t1.robot_id,'') != '' then t1.robot_id
             when nvl(t2.robot_id,'') != '' then t2.robot_id
             else ''
        end as robot_id,
        case when nvl(t1.tenant_id,'') != '' then t1.tenant_id
             when nvl(t2.tenant_id,'') != '' then t2.tenant_id
             else ''
        end as tenant_id,
        case when nvl(t1.robot_type,'') != '' then t1.robot_type
             when nvl(t2.robot_type,'') != '' then t2.robot_type
             else ''
        end as robot_type,
        case when nvl(t1.asr_vendor,'') != '' then t1.asr_vendor
             when nvl(t2.asr_vendor,'') != '' then t2.asr_vendor
             else ''
        end as asr_vendor,
        nvl(t2.asr_domain,'') as asr_domain,
        case when nvl(t1.event_time,'') != '' then t1.event_time
             when nvl(t2.event_time,'') != '' then t2.event_time
             else ''
        end as event_time,
        case when nvl(t1.k8s_env_name,'') != '' then t1.k8s_env_name
             when nvl(t2.k8s_env_name,'') != '' then t2.k8s_env_name
             else ''
        end as k8s_env_name,
        case when nvl(t1.dt,'') != '' then t1.dt
             when nvl(t2.dt,'') != '' then t2.dt
             else ''
        end as dt
    from (
         select
            question_id,
            robot_id,
            tenant_id,
            robot_type,
            asr_vendor,
            '' as asr_domain,
            event_time,
            k8s_env_name,
            dt
         from cdmdwd.dwd_harix_cmd_asr_recognize_i_d
         where dt >= '${s_dt_var}' and dt <= '${e_dt_var}' and k8s_env_name LIKE '%prod%'
    ) t1 left join (
        select
            question_id,
            robot_id,
            tenant_id,
            robot_type,
            asr_vendor,
            asr_domain,
            event_time,
            k8s_env_name,
            dt
        from cdmdwd.dwd_asr_cmd_asr_hit_domain_i_d
        where dt >= '${s_dt_var}' and dt <= '${e_dt_var}' and k8s_env_name LIKE '%prod%'
    ) t2 on t1.question_id = t2.question_id and t1.dt = t2.dt
    where nvl(t1.question_id,'') != ''
),
asr_dim as (
    select
        t1.question_id,
        t1.tenant_id,
        nvl(t3.tenant_name,'') as tenant_name,
        case when nvl(t2.robot_type_id,'') != '' then nvl(t2.robot_type_id,'')
             when nvl(t4.robot_type_id,'') != '' then nvl(t4.robot_type_id,'')
             else ''
        end as robot_type_id,
        case when nvl(t2.robot_type_name,'') != '' then nvl(t2.robot_type_name,'')
             when nvl(t4.robot_type_name,'') != '' then nvl(t4.robot_type_name,'')
             else ''
        end as robot_type_name,
        case when nvl(robot_type,'') != '' then nvl(robot_type,'')
             when nvl(t2.robot_type_inner_name,'') != '' then nvl(t2.robot_type_inner_name,'')
             when nvl(t4.robot_type_inner_name,'') != '' then nvl(t4.robot_type_inner_name,'')
             else ''
        end as robot_type_inner_name,
        asr_vendor,
        asr_domain,
        k8s_env_name,
        dt
    from merge_asr t1
    left join (
        select robot_id, robot_type_id, robot_type_name, robot_type_inner_name, start_time, end_time from cdmdim.dim_pmd_robot_sh_d where dt = '${e_dt_var}'
    ) t2 on t1.robot_id = t2.robot_id
    left join (
        select tenant_id, tenant_name, start_time, end_time from cdmdim.dim_umd_tenant_sh_d
    ) t3 on t1.tenant_id = t3.tenant_id
    left join (
        select robot_type_id, robot_type_name, robot_type_inner_name from cdmdim.dim_pmd_robot_type_dict_a_d
    ) t4 on t1.robot_type = t4.robot_type_inner_name
    where t1.event_time >= t2.start_time and t1.event_time <= t2.end_time
    and t1.event_time >= t3.start_time and t1.event_time < t3.end_time
)
insert overwrite table cdmads.ads_cmd_asr_recognize_statics_i_d partition(dt)
    select
        tenant_id,
        tenant_name,
        robot_type_id,
        robot_type_name,
        robot_type_inner_name,
        asr_vendor,
        asr_domain,
        k8s_env_name,
        count(1) as asr_recognize_cnt_sum_1d,
        dt
    from asr_dim
    group by tenant_id, tenant_name, robot_type_id, robot_type_name, robot_type_inner_name, asr_vendor, asr_domain, k8s_env_name, dt;