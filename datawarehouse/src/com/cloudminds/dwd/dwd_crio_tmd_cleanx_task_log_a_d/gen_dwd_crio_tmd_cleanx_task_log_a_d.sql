set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;
set mapreduce.job.queuename=root.users.liuhao;
with current_date_cleanx_task_log as (
   select
     cast(id as string) as log_id,
     nvl(tenant_code,'') as tenant_id,
     nvl(robot_type,'') as robot_type,
     nvl(robot_code,'') as robot_id,
     nvl(robot_name,'') as robot_name,
     case task_type when 0 then 'fixed'
                    when 1 then 'loop'
                    when -1 then '默认值'
                    else 'unknown' end as schedule_type,
     nvl(work_types,'') as work_types,
     regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(work_types,'0','洗地'),'1','吸污'),'2','扫地'),'3','广告'),'4','尘推') as work_type_names,
     nvl(task_code,'') as task_code,
     nvl(task_name,'') as task_name,
     cast(area_finish as decimal(8,2)) as area_finish,
     cast(area_plan as decimal(8,2)) as area_plan,
     cast(water as decimal(8,3)) as water,
     cast(distance as decimal(8,2)) as distance,
     cast(`percent` as decimal(5,2)) as `percent`,
     cast(battery as decimal(5,2)) as battery,
     cast(efficiency as decimal(8,2)) as efficiency,
     cast(speed as decimal(8,2)) as speed,
     CONCAT(start_time,".000") as start_time,
     CONCAT(end_time,".000") as end_time,
     version,
     if(bigdata_method='DELETE',44,status) as status,
     case status when -1 then '未执行'
                 when 0 then '启动任务成功'
                 when 1 then '操作者取消任务成功'
                 when 2 then '重启任务超时(任务取消)'
                 when 3 then '机器人取消任务成功'
                 when 4 then '暂停任务'
                 when 5 then '恢复任务'
                 when 10 then '任务完成'
                 when 44 then '删除'
                 else '未知状态' end as status_name,
     nvl(map_code,'') as map_code,
     nvl(map_name,'') as map_name,
     CONCAT(from_utc_timestamp(create_time,'GMT'),".000") as create_time,
     CONCAT(from_utc_timestamp(update_time,'GMT'),".000") as update_time,
     nvl(task_id, '') as task_id,
     nvl(cleanx_id, '') as cleanx_id,
     CASE WHEN schedule_time IS NOT NULL AND schedule_time != '' then concat(schedule_time,':00') else '' end as schedule_start_time,
     nvl(task_uuid,'') as task_uuid,
     nvl(record_state, -99999998) as record_state,
     case record_state when 0 then '启动成功'
                       when 1 then '启动录制失败'
                       when 2 then '停止录制成功'
                       when 3 then '停止录制失败'
                       when 9 then '不需要录制'
                       when -99999998 then '默认值null'
                       else '未知状态' end as record_state_name,
     nvl(event_flag, -99999998) as event_flag,
     case event_flag when 0 then '未生成'
                     when 1 then '已生成'
                     else '未知标识' end as event_flag_name,
     CONCAT(from_unixtime(cast(event_time/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.000') as event_time,
     'bj-prod-232' as k8s_env_name
   from cdmods.ods_crio_db_c0011_cleanx_task_log_i_d where dt='${e_dt_var}'
),
date_cleanx_task_log as (
   select
     *,
     row_number() over(partition by log_id ORDER BY update_time DESC) as rnk
   from (select * from cdmdwd.dwd_crio_tmd_cleanx_task_log_a_d union all select * from current_date_cleanx_task_log) a)
 ---存入数据
insert overwrite table cdmdwd.dwd_crio_tmd_cleanx_task_log_a_d
   select
      log_id,
      tenant_id,
      robot_type,
      robot_id,
      robot_name,
      schedule_type,
      work_types,
      work_type_names,
      task_code,
      task_name,
      area_finish,
      area_plan,
      water,
      distance,
      `percent`,
      battery,
      efficiency,
      speed,
      start_time,
      end_time,
      version,
      status,
      status_name,
      map_code,
      map_name,
      create_time,
      update_time,
      task_id,
      cleanx_id,
      schedule_start_time,
      task_uuid,
      record_state,
      record_state_name,
      event_flag,
      event_flag_name,
      event_time,
      k8s_env_name
   FROM date_cleanx_task_log where rnk=1;