set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;
set mapreduce.job.queuename=root.users.liuhao;
with current_date_ginger_task_log as (
   select
     cast(id as string) as log_id,
     nvl(task_id, '') as task_id,
     nvl(uuid, '') as uuid,
     nvl(task_uuid, '') as task_uuid,
     nvl(tenant_code, '') as tenant_id,
     nvl(tenant_name, '') as tenant_name,
     case robot_status when 1 then 0
                       else 1 end as robot_is_del,
     nvl(robot_type, '') as robot_type,
     nvl(robot_code, '') as robot_id,
     nvl(robot_name, '') as robot_name,
     nvl(rcu_code, '') as rcu_id,
     nvl(rcu_name, '') as rcu_name,
     nvl(map_code, '') as map_code,
     nvl(map_name, '') as map_name,
     nvl(task_type, -99999998) as work_type,
     case task_type when 0 then '智能服务机器人送货'
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
                    else '未知类型' end as work_type_name,
     nvl(task_code, '') as task_code,
     nvl(task_name, '') as task_name,
     CASE WHEN schedule_time IS NOT NULL AND schedule_time != '' then concat(schedule_time,':00') else '' end as schedule_start_time,
     nvl(concat(from_unixtime(cast(start_timestamp/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(start_timestamp,11)),'') as start_time,
     nvl(concat(from_unixtime(cast(end_timestamp/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(end_timestamp,11)),'') as end_time,
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
     nvl(task_mode, -99999998) as task_mode,
     case task_mode when null then '未设置值'
                    when 0 then '机器人发起'
                    when 1 then 'CROSS发起'
                    when 2 then '语音发起'
                    else '未知任务模式' end as task_mode_name,
     case dt when '2021-11-24' then CONCAT(from_utc_timestamp(create_time,'GMT+8'),".000")
             else CONCAT(from_utc_timestamp(create_time,'GMT'),".000") end as create_time,
     case dt when '2021-11-24' then CONCAT(from_utc_timestamp(update_time,'GMT+8'),".000")
                  else CONCAT(from_utc_timestamp(update_time,'GMT'),".000") end as update_time,
     nvl(record_state, -99999998) as record_state,
     case record_state when 0 then '启动成功'
                       when 1 then '启动录制失败'
                       when 2 then '停止录制成功'
                       when 3 then '停止录制失败'
                       when 9 then '不需要录制'
                       when -99999998 then '默认值null'
                       else '未知状态' end as record_state_name,
     nvl(work_data, '') as work_data,
     nvl(error_code, '') as error_code,
     nvl(error_message, '') as error_message,
     nvl(error_detail, '') as error_detail,
     nvl(biz_type, -99999998) as biz_type,
     case biz_type when 0 then '智能服务机器人'
                   when 1 then '快递通系统'
                   else '未知的业务类型' end as biz_type_name,
     CONCAT(from_unixtime(cast(event_time/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.000') as event_time,
     'bj-prod-232' as k8s_env_name
   from cdmods.ods_crio_db_c0010_t_task_log_i_d where dt>='${s_dt_var}' and dt<='${e_dt_var}'),

date_ginger_task_log as (
   select
     *,
     row_number() over(partition by log_id ORDER BY update_time DESC) as rnk
   from (select * from cdmdwd.dwd_crio_tmd_t_task_log_a_d union all select * from current_date_ginger_task_log) a)
 ---存入数据
insert overwrite table cdmdwd.dwd_crio_tmd_t_task_log_a_d
   select
     log_id,
     task_id,
     uuid,
     task_uuid,
     tenant_id,
     tenant_name,
     robot_is_del,
     robot_type,
     robot_id,
     robot_name,
     rcu_id,
     rcu_name,
     map_code,
     map_name,
     work_type,
     work_type_name,
     task_code,
     task_name,
     schedule_start_time,
     start_time,
     end_time,
     status,
     status_name,
     task_mode,
     task_mode_name,
     create_time,
     update_time,
     record_state,
     record_state_name,
     work_data,
     error_code,
     error_message,
     error_detail,
     biz_type,
     biz_type_name,
     event_time,
     k8s_env_name
   FROM date_ginger_task_log where rnk=1;


