set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;
set mapreduce.job.queuename=root.users.liuhao;

insert overwrite table cdmdim.dim_pmd_robot_manufacturer_a_d
   select
      code as robot_manufacturer_id,
      name as robot_manufacturer_name,
      CONCAT(create_time,".000") as created_at,
      CONCAT(create_time,".000") as updated_at,
      'bj-prod-232' as k8s_env_name
   from cdmods.ods_crio_db_c0001_t_dict_s_d
   where dt = '${e_dt_var}' and type=2;