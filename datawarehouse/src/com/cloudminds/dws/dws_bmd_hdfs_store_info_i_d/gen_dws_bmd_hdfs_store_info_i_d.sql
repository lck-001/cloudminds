set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;
set mapreduce.job.queuename=root.prod;

insert overwrite table cdmdws.dws_bmd_hdfs_store_info_i_d partition(dt)
    select
      analyse_type_name,
      analyse_value,
      count(file_path) as file_count,
      sum(file_size) as total_size,
      dt
    from cdmdwd.dwd_bigdata_bmd_hdfs_store_info_i_d where dt='${e_dt_var}'
    group by analyse_type_name,analyse_value,dt;