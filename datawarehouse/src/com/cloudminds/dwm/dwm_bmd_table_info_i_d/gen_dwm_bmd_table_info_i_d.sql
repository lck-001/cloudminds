set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;
set mapreduce.job.queuename=root.prod;
---补主题和部门的信息
with pre_table as (
   select
     t.table_name,
     t.db_name,
     t.create_time,
     t.update_time,
     t.table_type,
     t.table_type_name,
     t.num_rows,
     t.total_size,
     t.columns,
     t.db_op,
     case when t.db_name='cdmods' and table_type=1 then 1
          when d.analyse_type is not null then d.analyse_type
          else 2 end as analyse_type,
     case when d.analyse_value is not null then d.analyse_value
          when db_name in ('cdmods','cdmdim','cdmdwm','cdmdws','cdmads') and table_type=1 then split(table_name,'_')[1]
          when db_name='cdmdwd' and table_type=1 then split(table_name,'_')[2]
          else '其它' end as analyse_value
   from (select * from cdmdwd.dwd_bigdata_bmd_table_info_i_d where dt='${e_dt_var}') t
   left join cdmdim.dim_bmd_db_analyse_a_manual d
   on t.table_type=d.db_type and t.db_name=d.name
),
---有些命名不规范导致解析异常的得矫正
deal_table as (
   select
     t.table_name,
     t.db_name,
     t.create_time,
     t.update_time,
     t.table_type,
     t.table_type_name,
     t.num_rows,
     t.total_size,
     t.columns,
     t.db_op,
     t.analyse_type,
     case when d.analyse_value is not null then d.analyse_value
          else t.analyse_value end as analyse_value
   from pre_table t
   left join cdmdim.dim_bmd_db_analyse_a_manual d
   on t.table_type=d.db_type and concat(t.db_name,'-',t.table_name)=d.name)

insert overwrite table cdmdwm.dwm_bmd_table_info_i_d partition(dt)
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
      if(analyse_type=1,'部门','主题') as analyse_type_name,
      analyse_value,
      db_op,
      '${e_dt_var}' as dt
    from deal_table;