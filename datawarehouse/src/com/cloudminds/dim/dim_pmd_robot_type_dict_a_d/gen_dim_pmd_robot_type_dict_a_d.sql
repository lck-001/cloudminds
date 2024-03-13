set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;
set mapreduce.input.fileinputformat.input.dir.recursive=true;
set  hive.exec.max.dynamic.partitions.pernode=500;
set hive.merge.smallfiles.avgsize=128000000;
set hive.merge.size.per.task=128000000;
set mapreduce.job.queuename=root.users.liuhao;

with roc_t_dict as (
    select
        t1.robot_type_id,
        t1.robot_type_name,
        t2.robot_type_inner_name,
        case when t1.create_time < t2.create_time then t1.create_time
             else t2.create_time
        end as create_time,
        case when t1.update_time < t2.update_time then t2.update_time
             else t1.update_time
        end as update_time
    from (
             select
                 label as robot_type_name,
                 `value` as robot_type_id,
                  case when nvl(create_time,'') != '' and length(create_time) = 19 then concat(create_time,'.000')
                       else ''
                  end as create_time,
                  case when nvl(update_time,'') != '' and length(update_time) = 19 then concat(update_time,'.000')
                       else ''
                  end as update_time
             from cdmods.ods_roc_db_c0002_t_dict_s_d
             where dt = '${e_dt_var}' and TYPE = 'robot-type'
             group by label, `value`, create_time, update_time
         ) t1
             full join (
        select
            label as robot_type_inner_name,
            `value` as robot_type_id,
            case when nvl(create_time,'') != '' and length(create_time) = 19 then concat(create_time,'.000')
                 else ''
            end as create_time,
            case when nvl(update_time,'') != '' and length(update_time) = 19 then concat(update_time,'.000')
               else ''
            end as update_time
        from cdmods.ods_roc_db_c0002_t_dict_s_d
        where dt = '${e_dt_var}' and TYPE = 'robot-type-hari'
        group by label, `value`, create_time, update_time
    ) t2 on t1.robot_type_id = t2.robot_type_id
)
insert overwrite table cdmdim.dim_pmd_robot_type_dict_a_d
    select
        robot_type_id,
        robot_type_name,
        robot_type_inner_name,
        create_time,
        update_time
    from roc_t_dict
    distribute by 1;