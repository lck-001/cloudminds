set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;
set mapreduce.job.queuename=root.users.liuhao;

insert overwrite table cdmdim.dim_umd_industry_a_d
    select
       code as industry_id,
       name as industry_name,
       CONCAT(create_time,".000") as create_time,
       CONCAT(create_time,".000") as update_time,
       'bj-prod-232' as k8s_env_name
    from cdmods.ods_crio_db_c0001_t_dict_s_d
    where (type=10 or type =4) and dt = '${e_dt_var}';