set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.merge.smallfiles.avgsize=128000000;
set hive.merge.size.per.task=128000000;
set mapreduce.job.queuename=root.users.liuhao;

insert overwrite table cdmdim.dim_cmd_action_a_d
    select
        nvl(algo_intent,'') as action_id,
        nvl(intent_label,'') as intent_label,
        nvl(md5(concat(algo_intent,robot_type)),'') as action_md,
        nvl(trim(robot_type),'') as robot_type_inner_name,
        nvl(robot_intent,'') as robot_intent,
        case when nvl(created_time,'') != '' and length(created_time) = 19 then concat(created_time,'.000')
             else ''
        end as create_time,
        case when nvl(updated_time,'') != '' and length(updated_time) = 19 then concat(updated_time,'.000')
             else ''
        end as update_time,
        'bj-prod-232' as k8s_env_name
    from cdmods.ods_sv_db_c0003_dance_mapping_s_d
    where dt = '${e_dt_var}';