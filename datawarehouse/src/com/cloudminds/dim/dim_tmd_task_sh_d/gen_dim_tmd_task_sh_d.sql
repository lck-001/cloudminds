set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;
set mapreduce.job.queuename=root.users.liuhao;

---清洁机器人
with clean as (
  select
    id,robot_code
  from cdmods.ods_crio_db_c0011_cleanx_i_d group by id,robot_code),
     --cleanxTaskBinlog
     cleanx_task_binlog as (
     select
        cast(u.id as string) as task_id,
        nvl(u.name,'') as task_name,
        c.robot_code as robot_id,
        nvl(u.map_code,'') as map_id,
        nvl(u.map_name,'') as map_name,
        3 as task_type_id,
        '清洁' as task_type_name,
        nvl(u.work_types,'') as work_types,
        regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(u.work_types,'0','洗地'),'1','吸污'),'2','扫地'),'3','广告'),'4','尘推') as work_type_names,
        case u.task_type when 0 then 'fixed'
                       when 1 then 'loop'
                       else 'unknown' end as schedule_type,
        CASE WHEN u.start_time IS NOT NULL AND u.start_time != '' then concat(u.start_time,':00') else '' end as schedule_start_time,
        CASE WHEN u.end_time IS NOT NULL AND u.end_time != '' then concat(u.end_time,':00') else '' end as schedule_end_time,
        nvl(u.days,'') as schedule_days,
        u.repeat_times as repeat_times,
        u.repeat_type as repeat_type,
        case u.repeat_type when 1 then '日循环'
                         when 7 then '周循环'
                         when 30 then '月循环'
                         else '未知循环' end as repeat_type_name,
        case bigdata_method when 'INSERT' then CONCAT(from_utc_timestamp(create_time,'GMT'),".000")
                            when 'UPDATE' then CONCAT(from_utc_timestamp(update_time,'GMT'),".000")
                            else CONCAT(from_unixtime(cast(event_time/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.000') end as start_time,
        nvl(u.sub_tasks,'') as sub_task,
        case bigdata_method when 'INSERT' then 0
                            when 'UPDATE' then 0
                            else 1 end as is_del,
        CONCAT(from_utc_timestamp(u.create_time,'GMT'),".000") as create_time,
        if(bigdata_method='DELETE',concat(from_unixtime(cast(event_time/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.000'),CONCAT(from_utc_timestamp(update_time,'GMT'),".000")) as update_time
     from (select * from cdmods.ods_crio_db_c0011_cleanx_task_i_d where dt='${e_dt_var}') u left join clean c on u.cleanx_id=c.id),
    ---取前天的数据
    cleanx_task_old as(
    select
       task_id,
       task_name,
       robot_id,
       map_id,
       map_name,
       task_type_id,
       task_type_name,
       work_types,
       work_type_names,
       schedule_type,
       schedule_start_time,
       schedule_end_time,
       schedule_days,
       repeat_times,
       repeat_type,
       repeat_type_name,
       start_time,
       sub_task,
       is_del,
       create_time,
       update_time
    from cdmdim.dim_tmd_task_sh_d where dt=date_sub('${e_dt_var}', 1) and task_type_id=3),
  ---两者合并去重
 union_clean_data as (
 select
    *,
    lead(update_time,1,NULL) over(partition BY task_id ORDER BY update_time ASC) tmp_end_time
    from (
      select
        *,
        row_number() over(partition by
                              task_id,
                              task_name,
                              robot_id,
                              map_id,
                              map_name,
                              work_types,
                              schedule_type,
                              schedule_start_time,
                              schedule_end_time,
                              schedule_days,
                              repeat_times,
                              repeat_type,
                              is_del,
                              sub_task ORDER BY update_time ASC) as rnk
     from (select * from cleanx_task_binlog union all select * from cleanx_task_old) t) a where rnk=1),
  --最终清洁数据
 final_clean_data as (
  select
    task_id,
    task_name,
    robot_id,
    map_id,
    map_name,
    task_type_id,
    task_type_name,
    work_types,
    work_type_names,
    schedule_type,
    schedule_start_time,
    schedule_end_time,
    schedule_days,
    repeat_times,
    repeat_type,
    repeat_type_name,
    sub_task,
    is_del,
    create_time,
    update_time,
    start_time,
    CASE WHEN tmp_end_time IS NOT NULL AND tmp_end_time != '' THEN concat(from_unixtime(cast(substring(cast(unix_timestamp(substring(tmp_end_time,0,19) ,'yyyy-MM-dd HH:mm:ss') as bigint)*1000+cast(substring(tmp_end_time,21,23) as bigint) - 1,0,10) AS BIGINT),'yyyy-MM-dd HH:mm:ss'),'.',substring(cast(unix_timestamp(substring(tmp_end_time,0,19) ,'yyyy-MM-dd HH:mm:ss') as bigint)*1000+cast(substring(tmp_end_time,21,23) as bigint) - 1,11,13))
         ELSE '9999-12-31 23:59:59.999'
         END AS end_time,
    '${e_dt_var}' as dt
  FROM union_clean_data),



---安保数据
--最新的任务和子任务关系
task_and_navpoint as(
     select
     taskid,
     navpointid,
     case bigdata_method when 'INSERT' then 1
                         when 'UPDATE' then 2
                         else 3 end as db_op,
     CASE WHEN bigdata_method='DELETE' THEN nvl(concat(from_unixtime(cast((event_time-1)/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(event_time-1,11)),'')
          ELSE nvl(concat(from_unixtime(cast(event_time/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(event_time,11)),'')
          END AS event_time,
     row_number() over (partition by
                        taskid,
                        navpointid,
                        bigdata_method,
                        event_time) as rnk
  from cdmods.ods_crio_db_c0013_task_and_navpoint_i_d where dt<='${e_dt_var}'),

task_and_navpoint_remove_duplicate as (
    select
      taskid as task_id,
      navpointid as navpoint_id,
      event_time
    from (
     select
         *,
         row_number() over(partition by taskid,navpointid ORDER BY event_time desc) as new_rnk
	 from  task_and_navpoint where rnk=1 ) as tt where new_rnk=1 and db_op!=3),

 task_subTask as (
   select
     task_id,
     regexp_replace(concat_ws(",",collect_list(regexp_replace(navpoint,'"',"'"))),"'",'"') as sub_task
   from(
     select
        t.task_id as task_id,
        concat('{"pointName":"',n.navpoint_name,'","stayTime":',n.wait_time,',',substr(fn,2,length(fn)-2),',"x":',n.pos_x,',"y":',n.pos_y,'}') as navpoint
     from task_and_navpoint_remove_duplicate t left join (select * from cdmdim.dim_tmd_patrol_navpoint_task_sh_d where dt='${e_dt_var}' and is_del=0) n on t.navpoint_id=n.navpoint_id  where n.start_time is null or t.event_time between n.start_time and n.end_time) tt
   group by task_id
 ),
 ---binlog数据
  psp_robot_task_binlog as (
    select
      cast(p.id as string) as task_id,
      nvl(p.name,'') as task_name,
      p.robotid as robot_id,
      nvl(p.mapid,'') as map_id,
      nvl(p.mapname,'') as map_name,
      4 as task_type_id,
      '安保' as task_type_name,
      '' as work_types,
      '' as work_type_names,
       case p.type when 1 then 'loop'
                 when 2 then 'fixed'
                 else 'unknown' end as schedule_type,
      CASE WHEN p.starttime IS NOT NULL AND p.starttime != '' then p.starttime else '' end as schedule_start_time,
      CASE WHEN p.endtime IS NOT NULL AND p.endtime != '' then p.endtime else '' end as schedule_end_time,
      '1,2,3,4,5,6,7' as schedule_days,
      p.times as repeat_times,
      1 as repeat_type,
      '日循环' as repeat_type_name,
      if(nvl(t.sub_task,'')=='','',concat('[',t.sub_task,']')) as sub_task,
      if(bigdata_method='DELETE' or isdel=2,1,0) as is_del,
      case p.bigdata_method when 'INSERT' then CONCAT(from_utc_timestamp(p.pubtime,'GMT'),".000")
                          else concat(from_utc_timestamp(from_unixtime(cast(p.event_time/1000 as int),'yyyy-MM-dd HH:mm:ss'),'GMT+8'),'.000') end as start_time,
      CONCAT(from_utc_timestamp(p.pubtime,'GMT'),".000") as create_time,
      concat(from_utc_timestamp(from_unixtime(cast(p.event_time/1000 as int),'yyyy-MM-dd HH:mm:ss'),'GMT+8'),'.000') as update_time
    from (select * from cdmods.ods_crio_db_c0013_robot_task_i_d where dt='${e_dt_var}') p left join task_subTask t on p.id=t.task_id),
 ---取前天的数据
    psp_task_old as(
    select
       task_id,
       task_name,
       robot_id,
       map_id,
       map_name,
       task_type_id,
       task_type_name,
       work_types,
       work_type_names,
       schedule_type,
       schedule_start_time,
       schedule_end_time,
       schedule_days,
       repeat_times,
       repeat_type,
       repeat_type_name,
       sub_task,
       is_del,
       start_time,
       create_time,
       update_time
    from cdmdim.dim_tmd_task_sh_d where dt=date_sub('${e_dt_var}', 1) and task_type_id=4),
---两者合并去重
 union_psp_data as (
 select
    *,
    lead(update_time,1,NULL) over(partition BY task_id ORDER BY update_time ASC) tmp_end_time
    from (
      select
        *,
        row_number() over(partition by
                              task_id,
                              task_name,
                              robot_id,
                              map_id,
                              map_name,
                              schedule_type,
                              schedule_start_time,
                              schedule_end_time,
                              repeat_times,
                              is_del ORDER BY update_time ASC) as rnk
     from (select * from psp_robot_task_binlog union all select * from psp_task_old) t) a where rnk=1),
--最终安保数据
 final_psp_data as (
  select
    task_id,
    task_name,
    robot_id,
    map_id,
    map_name,
    task_type_id,
    task_type_name,
    work_types,
    work_type_names,
    schedule_type,
    schedule_start_time,
    schedule_end_time,
    schedule_days,
    repeat_times,
    repeat_type,
    repeat_type_name,
    sub_task,
    is_del,
    create_time,
    update_time,
    start_time,
    CASE WHEN tmp_end_time IS NOT NULL AND tmp_end_time != '' THEN concat(from_unixtime(cast(substring(cast(unix_timestamp(substring(tmp_end_time,0,19) ,'yyyy-MM-dd HH:mm:ss') as bigint)*1000+cast(substring(tmp_end_time,21,23) as bigint) - 1,0,10) AS BIGINT),'yyyy-MM-dd HH:mm:ss'),'.',substring(cast(unix_timestamp(substring(tmp_end_time,0,19) ,'yyyy-MM-dd HH:mm:ss') as bigint)*1000+cast(substring(tmp_end_time,21,23) as bigint) - 1,11,13))
         ELSE '9999-12-31 23:59:59.999'
         END AS end_time,
    '${e_dt_var}' as dt
  FROM union_psp_data),


---Ginger机器人
 ginger_task_binlog as (
  select
    cast(id as string) as task_id,
    nvl(name,'') as task_name,
    nvl(robot_code,'') as robot_id,
    nvl(map_code,'') as map_id,
    nvl(map_name,'') as map_name,
    1 as task_type_id,
    'Ginger任务' as task_type_name,
    cast(biz_task_type as string) as work_types,
    case biz_task_type when 0 then '智能服务机器人送货'
                       when 1 then '智能服务机器人引领'
                       when 2 then '智能服务机器人闹钟'
                       when 10 then '快递通寄件'
                       when 11 then '快递通派件'
                       when 12 then '消杀任务'
                       when 13 then '导览任务'
                       when 14 then '单点呼叫'
                       when 15 then '沉浸式培训'
                       when 16 then '远程控制设备'
                       when 17 then '巡检'
                       when 18 then '送餐'
                       when 19 then '回厨房'
                       when 20 then '收厨余'
                       when 21 then '送厨余'
                       when 22 then '迎宾'
                       when 23 then '领位'
                       else 'unknown' end as work_type_names,
    case task_type when 0 then 'fixed'
                       when 1 then 'loop'
                       else 'unknown' end as schedule_type,
    CASE WHEN start_time IS NOT NULL AND start_time != '' then concat(start_time,':00') else '' end as schedule_start_time,
    CASE WHEN end_time IS NOT NULL AND end_time != '' then concat(end_time,':00') else '' end as schedule_end_time,
    nvl(days,'') as schedule_days,
    repeat_times as repeat_times,
    repeat_type as repeat_type,
    case repeat_type when 1 then '日循环'
                     when 7 then '周循环'
                     when 30 then '月循环'
                     else '未知循环' end as repeat_type_name,
    case bigdata_method when 'INSERT' then CONCAT(from_utc_timestamp(create_time,'GMT'),".000")
                        when 'UPDATE' then CONCAT(from_utc_timestamp(update_time,'GMT'),".000")
                        else CONCAT(from_unixtime(cast(event_time/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.000') end as start_time,
    nvl(sub_tasks,'') as sub_task,
    case bigdata_method when 'INSERT' then 0
                            when 'UPDATE' then 0
                            else 1 end as is_del,
    CONCAT(from_utc_timestamp(create_time,'GMT'),".000") as create_time,
    if(bigdata_method='DELETE',CONCAT(from_unixtime(cast(event_time/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.000'),CONCAT(from_utc_timestamp(update_time,'GMT'),".000")) as update_time
  from cdmods.ods_crio_db_c0010_t_task_i_d where dt='${e_dt_var}'),
  ---取前天的数据
     ginger_task_old as(
     select
        task_id,
        task_name,
        robot_id,
        map_id,
        map_name,
        task_type_id,
        task_type_name,
        work_types,
        work_type_names,
        schedule_type,
        schedule_start_time,
        schedule_end_time,
        schedule_days,
        repeat_times,
        repeat_type,
        repeat_type_name,
        start_time,
        sub_task,
        is_del,
        create_time,
        update_time
     from cdmdim.dim_tmd_task_sh_d where dt=date_sub('${e_dt_var}', 1) and task_type_id=1),
 ---两者合并去重
  union_ginger_data as (
  select
     *,
     lead(update_time,1,NULL) over(partition BY task_id ORDER BY update_time ASC) tmp_end_time
     from (
       select
         *,
         row_number() over(partition by
                               task_id,
                               task_name,
                               robot_id,
                               map_id,
                               map_name,
                               work_types,
                               schedule_type,
                               schedule_start_time,
                               schedule_end_time,
                               schedule_days,
                               repeat_times,
                               repeat_type,
                               is_del,
                               sub_task ORDER BY update_time ASC) as rnk
      from (select * from ginger_task_binlog union all select * from ginger_task_old) t) a where rnk=1),
 --最终ginger数据
  final_ginger_data as (
   select
     task_id,
     task_name,
     robot_id,
     map_id,
     map_name,
     task_type_id,
     task_type_name,
     work_types,
     work_type_names,
     schedule_type,
     schedule_start_time,
     schedule_end_time,
     schedule_days,
     repeat_times,
     repeat_type,
     repeat_type_name,
     sub_task,
     is_del,
     create_time,
     update_time,
     start_time,
     CASE WHEN tmp_end_time IS NOT NULL AND tmp_end_time != '' THEN concat(from_unixtime(cast(substring(cast(unix_timestamp(substring(tmp_end_time,0,19) ,'yyyy-MM-dd HH:mm:ss') as bigint)*1000+cast(substring(tmp_end_time,21,23) as bigint) - 1,0,10) AS BIGINT),'yyyy-MM-dd HH:mm:ss'),'.',substring(cast(unix_timestamp(substring(tmp_end_time,0,19) ,'yyyy-MM-dd HH:mm:ss') as bigint)*1000+cast(substring(tmp_end_time,21,23) as bigint) - 1,11,13))
          ELSE '9999-12-31 23:59:59.999'
          END AS end_time,
     '${e_dt_var}' as dt
   FROM union_ginger_data)

 ---存入数据
insert overwrite table cdmdim.dim_tmd_task_sh_d partition(dt)
   select
      task_id,
      task_name,
      robot_id,
      map_id,
      map_name,
      task_type_id,
      task_type_name,
      work_types,
      work_type_names,
      schedule_type,
      schedule_start_time,
      schedule_end_time,
      schedule_days,
      repeat_times,
      repeat_type,
      repeat_type_name,
      sub_task,
      is_del,
      create_time,
      update_time,
      start_time,
      end_time,
      dt
   FROM (select * from final_clean_data union all select * from final_psp_data union all select * from final_ginger_data) a;