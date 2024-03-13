set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;
set mapreduce.job.queuename=root.users.liuhao;

with new_navpoint_task as(
   select
       cast(id as string) as navpoint_id,
       nvl(name,'') as navpoint_name,
       nvl(robotid,'') as robot_id,
       nvl(pathid,-99999998) as path_id,
       nvl(pathname,'') as path_name,
       nvl(mapname,'') as map_name,
       nvl(waittime,-99999998) as wait_time,
       nvl(posx,-99999998) as pos_x,
       nvl(posy,-99999998) as pos_y,
       nvl(fn,'') as fn,
       nvl(remarks,'') as description,
       k8s_env_name as k8s_env_name,
       if(isdel=2,1,0) as is_del,
       nvl(concat(from_unixtime(cast(event_time/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(event_time,11)),'') as start_time,
       '' as end_time,
       '' as dt
   from cdmods.ods_crio_db_c0013_navpoint_task_i_d
   where dt = '${e_dt_var}'),

all_navpoint_task as (
     select *,
         row_number() over
         (partition by
               navpoint_id,
               navpoint_name,
               robot_id,
               path_id,
               path_name,
               map_name,
               wait_time,
               pos_x,
               pos_y,
               fn,
               description,
               k8s_env_name,
               is_del
         ) as rnk
	 from (select * from new_navpoint_task union all select * from cdmdim.dim_tmd_patrol_navpoint_task_sh_d where dt=date_sub('${e_dt_var}', 1)) as t),

final_navpoint_task as (
   select
     *,
     lead(start_time,1,NULL) over(partition BY navpoint_id ORDER BY start_time ASC) tmp_end_time
   from all_navpoint_task where rnk=1)

insert overwrite table cdmdim.dim_tmd_patrol_navpoint_task_sh_d partition(dt)
   select
     a.navpoint_id,
     a.navpoint_name,
     a.robot_id,
     a.path_id,
     a.path_name,
     a.map_name,
     a.wait_time,
     a.pos_x,
     a.pos_y,
     a.fn,
     a.description,
     a.k8s_env_name,
     a.is_del,
     a.start_time,
     CASE WHEN tmp_end_time IS NOT NULL AND tmp_end_time != '' THEN concat(from_unixtime(cast(substring(cast(unix_timestamp(substring(tmp_end_time,0,19) ,'yyyy-MM-dd HH:mm:ss') as bigint)*1000+cast(substring(tmp_end_time,21,23) as bigint) - 1,0,10) AS BIGINT),'yyyy-MM-dd HH:mm:ss'),'.',substring(cast(unix_timestamp(substring(tmp_end_time,0,19) ,'yyyy-MM-dd HH:mm:ss') as bigint)*1000+cast(substring(tmp_end_time,21,23) as bigint) - 1,11,13))
                    ELSE '9999-12-31 23:59:59.999'
                END AS end_time,
     '${e_dt_var}' as dt
   FROM final_navpoint_task a;