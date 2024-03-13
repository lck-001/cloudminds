set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;
set mapreduce.job.queuename=root.prod;

insert overwrite table cdmdws.dws_bmd_table_info_i_d partition(dt)
    select
      table_type_name,
      analyse_type_name,
      analyse_value,
      count(distinct db_name) as db_count,
      count(distinct table_name) as table_count,
      sum(size(split(columns,','))) as columns_count,
      sum(num_rows) as num_rows,
      sum(total_size) as total_size,
      '${e_dt_var}' as dt
    from cdmtmp.tmp_bmd_table_info_s_d_tanqiong_20780302 where dt='${e_dt_var}'
    group by table_type_name,analyse_type_name, analyse_value;