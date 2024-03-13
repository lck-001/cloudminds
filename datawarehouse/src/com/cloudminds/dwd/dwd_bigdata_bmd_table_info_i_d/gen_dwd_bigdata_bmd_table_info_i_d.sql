set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;
set mapreduce.job.queuename=root.prod;
---hive表新增的
with hive_old_table as (
     select
       *
     from cdmods.ods_bigdata_db_c0036_table_info_s_d
     where dt=date_sub('${e_dt_var}',1)
),
hive_new_table as (
     select
       *
     from cdmods.ods_bigdata_db_c0036_table_info_s_d
     where dt='${e_dt_var}'
),
hive_delete as (
     select
       o.tbl_name as table_name,
       o.db_name as db_name,
       concat(from_unixtime(o.create_time),'.000') as create_time,
       concat(from_unixtime(cast(nvl(o.last_modified_time,o.create_time) as int)),'.000') as update_time,
       1 as table_type,
       'hive' as table_type_name,
       cast(nvl(o.numRows,0) as bigint)+cast(nvl(o.partionNumRows,0) as bigint) as num_rows,
       cast(nvl(o.totalSize,0) as bigint)+cast(nvl(o.partionSize,0) as bigint) as total_size,
       o.columns as columns,
       3 as db_op
     from hive_old_table o left join hive_new_table n on o.tbl_name=n.tbl_name and o.db_name=n.db_name
     where n.tbl_name is null
),
hive_new_old_compare as (
   select
     n.tbl_name as table_name,
     n.db_name as db_name,
     concat(from_unixtime(n.create_time),'.000') as create_time,
     concat(from_unixtime(cast(nvl(n.last_modified_time,n.create_time) as int)),'.000') as update_time,
     1 as table_type,
     'hive' as table_type_name,
     cast(nvl(n.numRows,0) as bigint)+cast(nvl(n.partionNumRows,0) as bigint) as num_rows,
     cast(nvl(n.totalSize,0) as bigint)+cast(nvl(n.partionSize,0) as bigint) as total_size,
     n.columns,
     case when o.tbl_name is null then 1
               else if(nvl(o.numRows,'')=nvl(n.numRows,'') and nvl(o.totalSize,'')=nvl(n.totalSize,'') and nvl(o.partionNumRows,'')=nvl(n.partionNumRows,'') and nvl(o.partionSize,'')=nvl(n.partionSize,''),-1,2)
               end as db_op
   from hive_new_table n left join hive_old_table o on n.tbl_name=o.tbl_name and n.db_name=o.db_name
),
clickhouse_old_table as (
     select
       *
     from cdmods.ods_bigdata_db_c0037_table_info_s_d
     where dt=date_sub('${e_dt_var}',1)
),
clickhouse_new_table as (
     select
       *
     from cdmods.ods_bigdata_db_c0037_table_info_s_d
     where dt='${e_dt_var}'
),
clickhouse_delete as (
     select
       o.name as table_name,
       o.`database` as db_name,
       nvl(concat(from_unixtime(cast(o.metadata_modification_time/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(o.metadata_modification_time,11)),'') as create_time,
       nvl(concat(from_unixtime(cast(o.metadata_modification_time/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(o.metadata_modification_time,11)),'') as update_time,
       2 as table_type,
       'clickhouse' as table_type_name,
       nvl(o.total_rows,0) as num_rows,
       nvl(o.total_bytes,0) as total_size,
       regexp_replace(o.cloumns,'\'|\\[|\\]','') as columns,
       3 as db_op
     from clickhouse_old_table o left join clickhouse_new_table n on o.name=n.name and o.`database`=n.`database`
     where n.name is null
),
clickhouse_new_old_compare as (
   select
     n.name as table_name,
     n.`database` as db_name,
     nvl(concat(from_unixtime(cast(n.metadata_modification_time/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(n.metadata_modification_time,11)),'') as create_time,
     nvl(concat(from_unixtime(cast(n.metadata_modification_time/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(n.metadata_modification_time,11)),'') as update_time,
     2 as table_type,
     'clickhouse' as table_type_name,
     nvl(n.total_rows,0) as num_rows,
     nvl(n.total_bytes,0) as total_size,
     regexp_replace(n.cloumns,'\'|\\[|\\]','') as columns,
     case when o.name is null then 1
               else if(nvl(o.total_rows,0)=nvl(n.total_rows,0) and nvl(o.total_bytes,0)=nvl(n.total_bytes,0),-1,2)
               end as db_op
   from clickhouse_new_table n left join clickhouse_old_table o on n.name=o.name and n.`database`=o.`database`
),
clickhouse_hive_change as (
  select * from clickhouse_delete
  union all
  select * from clickhouse_new_old_compare where db_op!=-1
  union all
  select * from hive_delete
  union all
  select * from hive_new_old_compare where db_op!=-1
)
insert overwrite table cdmdwd.dwd_bigdata_bmd_table_info_i_d partition(dt)
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
      db_op,
      '${e_dt_var}' as dt
    from clickhouse_hive_change;
