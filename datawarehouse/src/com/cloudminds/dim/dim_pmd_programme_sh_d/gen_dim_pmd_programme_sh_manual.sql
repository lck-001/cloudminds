set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;
set mapreduce.job.queuename=root.users.liuhao;

with robot_task as (
    select
        programme_id,
        programme_name,
        programme_type,
        robot_id,
        task_start_time,
        task_end_time,
        times,
        path_id,
        task_path,
        map_id,
        map_name,
        status,
        status_name,
        is_del,
        is_changed,
        roc_task_status,
        pub_time,
        start_time,
        end_time,
        k8s_env_name,
        create_time,
        update_time
    from (
         select
             cast(id as string) as programme_id,
             name as programme_name,
             type as programme_type,
             robotid as robot_id,
             starttime as task_start_time,
             endtime as task_end_time,
             times,
             cast(pathid as string) as path_id,
             taskpath as task_path,
             cast(mapid as string) as map_id,
             mapname as map_name,
             case when status = true then 1
                  when status = false then 0
                  else -99999998
                 end as status,
             case when status = true then '完成'
                  when status = false then '未完成'
                  else '未知'
                 end as status_name,
             case when lower(trim(isdel)) = 'yes' then 1
                  else 0
                 end as is_del,
             case when lower(trim(taskstatus)) = 'changed' then 1
                  else 0
                 end as is_changed,
             taskstatusfan as roc_task_status,
             case when length(pubtime) = 10 then nvl(concat(from_unixtime( cast(pubtime as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
                  when length(pubtime) = 19 then nvl(concat(pubtime,'.000'),'')
                  when length(pubtime) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(pubtime,'T',' '),0,23),'')
                  else nvl(concat(from_unixtime( cast(pubtime/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(pubtime,11,3)),'')
                 end as pub_time,
             case when length(pubtime) = 10 then nvl(concat(from_unixtime( cast(pubtime as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
                  when length(pubtime) = 19 then nvl(concat(pubtime,'.000'),'')
                  when length(pubtime) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(pubtime,'T',' '),0,23),'')
                  else nvl(concat(from_unixtime( cast(pubtime/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(pubtime,11,3)),'')
                 end as start_time,
             '9999-12-31 23:59:59.999' as end_time,
             case when length(pubtime) = 10 then nvl(concat(from_unixtime( cast(pubtime as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
                  when length(pubtime) = 19 then nvl(concat(pubtime,'.000'),'')
                  when length(pubtime) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(pubtime,'T',' '),0,23),'')
                  else nvl(concat(from_unixtime( cast(pubtime/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(pubtime,11,3)),'')
                 end as create_time,
             case when length(pubtime) = 10 then nvl(concat(from_unixtime( cast(pubtime as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
                  when length(pubtime) = 19 then nvl(concat(pubtime,'.000'),'')
                  when length(pubtime) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(pubtime,'T',' '),0,23),'')
                  else nvl(concat(from_unixtime( cast(pubtime/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(pubtime,11,3)),'')
                 end as update_time,
             k8s_env_name
         from cdmods.ods_psp_db_c0013_robot_task_s_d where dt = '${e_dt_var}'
     ) t
),
programme_sh as (
    select
        programme_id,
        programme_name,
        programme_type,
        robot_id,
        task_start_time,
        task_end_time,
        times,
        path_id,
        task_path,
        map_id,
        map_name,
        status,
        status_name,
        is_del,
        is_changed,
        roc_task_status,
        pub_time,
        start_time,
        CASE WHEN tmp_end_time IS NOT NULL AND tmp_end_time != '' THEN concat(from_unixtime(cast((cast(unix_timestamp(substring(tmp_end_time,0,19),'yyyy-MM-dd HH:mm:ss') AS bigint)*1000 + cast(substring(tmp_end_time,21,23) AS bigint) - 1)/1000 as bigint),'yyyy-MM-dd HH:mm:ss'),'.',SUBSTRING((cast(unix_timestamp(substring(tmp_end_time,0,19),'yyyy-MM-dd HH:mm:ss') AS bigint)*1000 + cast(substring(tmp_end_time,21,23) AS bigint) - 1),11,13))
             ELSE '9999-12-31 23:59:59.999'
            END AS end_time,
        k8s_env_name,
        create_time,
        update_time
    from (
         select
             programme_id,
             programme_name,
             programme_type,
             robot_id,
             task_start_time,
             task_end_time,
             times,
             path_id,
             task_path,
             map_id,
             map_name,
             status,
             status_name,
             is_del,
             is_changed,
             roc_task_status,
             pub_time,
             start_time,
             end_time,
             k8s_env_name,
             create_time,
             update_time,
             lead(update_time,1,NULL) over(partition BY programme_id ORDER BY update_time ASC) tmp_end_time
         from (
              select
                  programme_id,
                  programme_name,
                  programme_type,
                  robot_id,
                  task_start_time,
                  task_end_time,
                  times,
                  path_id,
                  task_path,
                  map_id,
                  map_name,
                  status,
                  status_name,
                  is_del,
                  is_changed,
                  roc_task_status,
                  pub_time,
                  start_time,
                  end_time,
                  k8s_env_name,
                  create_time,
                  update_time
              from cdmdim.dim_pmd_programme_sh_d where dt = date_sub('${e_dt_var}',1)
              union all
              select
                  programme_id,
                  programme_name,
                  programme_type,
                  robot_id,
                  task_start_time,
                  task_end_time,
                  times,
                  path_id,
                  task_path,
                  map_id,
                  map_name,
                  status,
                  status_name,
                  is_del,
                  is_changed,
                  roc_task_status,
                  pub_time,
                  start_time,
                  end_time,
                  k8s_env_name,
                  create_time,
                  update_time
              from robot_task
          ) t
     ) t1
)
insert overwrite table cdmdim.dim_pmd_programme_sh_d partition(dt)
    select
        programme_id,
        programme_name,
        programme_type,
        robot_id,
        task_start_time,
        task_end_time,
        times,
        path_id,
        task_path,
        map_id,
        map_name,
        status,
        status_name,
        is_del,
        is_changed,
        roc_task_status,
        pub_time,
        start_time,
        end_time,
        k8s_env_name,
        create_time,
        update_time,
        '${e_dt_var}'
    from programme_sh;
