set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;
set mapreduce.job.queuename=root.prod;

with convert_hdfs_info as (
     select
       owner as creator_name,
       path  as file_path,
       concat(createTime,':00.000') as create_time,
       concat(modifiedTime,':00.000') as update_time,
       if(isFile,1,2) as file_type,
       if(isFile,'文件','目录') as file_type_name,
       fileSize as file_size,
       case when split(department,'/')[2] in ('source','cdm','ods') then split(department,'/')[3]
            else split(department,'/')[2] end as db_name,
       department,
       split(department,'/')[3] as table_name
     from cdmods.ods_bigdata_db_c0032_fsimage_i_d
     where dt>='${s_dt_var}' and dt<='${e_dt_var}'
),
pre_hdfs_info as (
    select
      h.creator_name,
      h.file_path,
      h.create_time,
      h.update_time,
      h.file_type,
      h.file_type_name,
      h.file_size,
      h.db_name,
      h.table_name,
      case when h.db_name='cdmods' then 1
                 when d.analyse_type is not null then d.analyse_type
                 else 2 end as analyse_type,
            case when d.analyse_value is not null then d.analyse_value
                 when h.db_name in ('cdmods','cdmdim','cdmdwm','cdmdws','cdmads') then split(table_name,'_')[1]
                 when db_name='cdmdwd' then split(table_name,'_')[2]
                 else '其它' end as analyse_value
    from convert_hdfs_info h left join (select * from cdmdim.dim_bmd_db_analyse_a_manual where db_type=1) d
    on h.db_name=d.name
),
deal_hdfs_info as (
     select
       h.creator_name,
       h.file_path,
       h.create_time,
       h.update_time,
       h.file_type,
       h.file_type_name,
       h.file_size,
       h.analyse_type,
       if(h.analyse_type=1,'部门','主题') as analyse_type_name,
       case when d.analyse_value is not null then d.analyse_value
             else h.analyse_value end as analyse_value,
       substring(h.create_time,1,10) as dt
     from pre_hdfs_info h left join (select * from cdmdim.dim_bmd_db_analyse_a_manual where db_type=1) d
     on concat(h.db_name,'-',h.table_name)=d.name
),
---合并头一天的数据并把重复的打上rnk
final_hdfs_info as (
     select
       *,
       row_number() over (partition by file_path order by create_time asc) as rnk
     from (select * from deal_hdfs_info union all select * from cdmdwd.dwd_bigdata_bmd_hdfs_store_info_i_d where dt='${s_dt_var}' or dt=date_sub('${s_dt_var}',1)) t
)
insert overwrite table cdmdwd.dwd_bigdata_bmd_hdfs_store_info_i_d partition(dt)
    select
      creator_name,
      file_path,
      create_time,
      update_time,
      file_type,
      file_type_name,
      file_size,
      analyse_type,
      if(analyse_type=1,'部门','主题') as analyse_type_name,
      analyse_value,
      dt
    from final_hdfs_info where rnk=1;