set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;
set mapreduce.job.queuename=root.prod;

with tb_info as (
     select
       o.table_name,
       o.db_name,
       if(n.table_name is null,o.create_time,n.create_time) as create_time,
       if(n.table_name is null,o.update_time,n.update_time) as update_time,
       o.table_type,
       o.table_type_name,
       if(n.table_name is null,o.num_rows,n.num_rows) as num_rows,
       if(n.table_name is null,o.total_size,n.total_size) as total_size,
       if(n.table_name is null,o.columns,n.columns) as columns,
       if(n.table_name is null,o.analyse_type,n.analyse_type) as analyse_type,
       if(n.table_name is null,o.analyse_type_name,n.analyse_type_name) as analyse_type_name,
       if(n.table_name is null,o.analyse_value,n.analyse_value) as analyse_value,
       1 as dp_op,
       '${e_dt_var}' as dt
     from (select * from cdmtmp.tmp_bmd_table_info_s_d_tanqiong_20780302 where dt=date_sub('${e_dt_var}',1)) o left join (select * from cdmdwm.dwm_bmd_table_info_i_d where dt='${e_dt_var}') n
     on o.table_name=n.table_name and o.db_name=n.db_name and o.table_type=n.table_type where nvl(n.db_op,-1)!=3
)
insert overwrite table cdmtmp.tmp_bmd_table_info_s_d_tanqiong_20780302 partition(dt)
    select
      table_name,
      db_name,
      create_time,
      update_time,
      table_type,
      table_type_name,
      num_rows,
      total_size,
      columns,
      analyse_type,
      analyse_type_name,
      analyse_value,
      '${e_dt_var}' as dt
    from (select * from tb_info union all select * from cdmdwm.dwm_bmd_table_info_i_d where dt='${e_dt_var}' and db_op=1) t;