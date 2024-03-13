set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;
set mapreduce.job.queuename=root.prod;

insert overwrite table cdmdws.dws_vmd_robot_detect_video_info_i_d partition(dt)
    select
      type_name,
      k8s_env_name,
      if(tenant__tenant_name='','未知',tenant__tenant_name) as tenant_name,
      if(tenant__tenant_type_name='','未知',tenant__tenant_type_name) as tenant_type_name,
      if(tenant__industry_name='','未知',tenant__industry_name) as tenant_industry_name,
      count(1) as image_count,
      dt
    from cdmdwm.dwm_vmd_robot_detect_video_info_i_d where dt>=date_sub('${s_dt_var}',1) and dt<='${e_dt_var}'
    group by type_name,k8s_env_name,tenant__tenant_name,tenant__tenant_type_name,tenant__industry_name,dt;