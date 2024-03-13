set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;
set mapreduce.job.queuename=root.users.liuhao;

with event_tmp as (
  select
      nvl(get_json_object(json_extract,'$.json_extract.guid'),'') as event_id,
      nvl(get_json_object(json_extract,'$.json_extract.robotId'),'') as robot_id,
      nvl(get_json_object(json_extract,'$.json_extract.rcuId'),'') as rcu_id,
      nvl(get_json_object(json_extract,'$.json_extract.accountId'),'') as robot_account_id,
      nvl(get_json_object(json_extract,'$.json_extract.tenantId'),'') as tenant_id,
      nvl(substr(regexp_replace(get_json_object(json_extract,'$.json_extract.timestamp'),"T"," "),1,23),'') as event_time,
      nvl(get_json_object(json_extract,'$.json_extract.source'),'') as source,
      nvl(get_json_object(json_extract,'$.json_extract.data.robot.mapInfo.mapId'),'') as map_id,
      nvl(get_json_object(json_extract,'$.json_extract.data.robot.mapInfo.mapName'),'') as map_name,
      if(get_json_object(json_extract,'$.json_extract.data.robot.bigLight.state')='true',1,0) as big_light_status,
      if(get_json_object(json_extract,'$.json_extract.data.robot.whistleWarning.state')='true',1,0) as whistle_warning_status,
      if(get_json_object(json_extract,'$.json_extract.data.robot.chargingPile.state')='true',1,0) as charging_pile_status,
      if(get_json_object(json_extract,'$.json_extract.data.robot.chargingOnLine.state')='true',1,0) as charging_online_status,
      if(get_json_object(json_extract,'$.json_extract.data.robot.battery.chargingStatus')='1',1,0) as battery_charging_status,
      cast(nvl(get_json_object(json_extract,'$.json_extract.data.config.navi.targetSpeed'),'0') as int) as navi_target_speed,
      nvl(get_json_object(json_extract,'$.json_extract.data.config.navi.patrollingRouteName'),'') as navi_patrolling_route_name,
      if(get_json_object(json_extract,'$.json_extract.data.config.navi.emergencyStopSwitch')='on',1,0) as navi_emergency_stop_switch_status,
      if(get_json_object(json_extract,'$.json_extract.data.config.navi.warningLight')='on',1,0) as navi_warning_light_status,
      cast(nvl(get_json_object(json_extract,'$.json_extract.data.config.navi.speed'),'0') as int) as navi_speed,
      cast(nvl(get_json_object(json_extract,'$.json_extract.data.robot.battery.percent'),'0') as int) as current_battery,
      nvl(get_json_object(json_extract,'$.json_extract.data.config.chassisEnvironment.atmosphere'),'') as atmosphere,
      nvl(get_json_object(json_extract,'$.json_extract.data.config.chassisEnvironment.pm25'),'') as pm25,
      nvl(get_json_object(json_extract,'$.json_extract.data.config.chassisEnvironment.CO'),'') as co,
      nvl(get_json_object(json_extract,'$.json_extract.data.config.chassisEnvironment.CO2'),'') as co2,
      nvl(get_json_object(json_extract,'$.json_extract.data.config.chassisEnvironment.temperature'),'') as temperature,
      nvl(get_json_object(json_extract,'$.json_extract.data.config.chassisEnvironment.humidity'),'') as humidity,
      nvl(get_json_object(json_extract,'$.json_extract.data.config.chassisEnvironment.frog'),'') as fog,
      nvl(get_json_object(json_extract,'$.json_extract.data.config.chassisEnvironment.TVOC'),'') as tvoc,
      if(get_json_object(json_extract,'$.json_extract.data.config.cameraConnectionState.top')='on',1,0) as top_camera_connection_state,
      k8s_env_name as k8s_env_name,
      substr(get_json_object(json_extract,'$.json_extract.timestamp'),1,10) as dt
  from hari.harix_etl_es
  where tdate>='${s_dt_var}'
  and tdate<='${e_dt_var}'
  and get_json_object(json_extract,'$.json_extract.robotType')='patrol'
  and get_json_object(json_extract,'$.json_extract.moduleId')='reportStatus'
  and json_extract like "%chassisEnvironment%"
),
---去重
event_remove_duplicate as (
  select
      *,
      row_number() over (partition by event_id order by event_time asc) as rank
  from (
       select
           *
       from event_tmp
       union all
       select
           *
       from cdmdwd.dwd_harix_omd_patrol_sensor_info_i_d
       where dt=date_sub('${s_dt_var}',1)
       ) t
)
insert overwrite table cdmdwd.dwd_harix_omd_patrol_sensor_info_i_d partition(dt)
    select
        event_id,
        robot_id,
        rcu_id,
        robot_account_id,
        tenant_id,
        event_time,
        source,
        map_id,
        map_name,
        big_light_status,
        whistle_warning_status,
        charging_pile_status,
        charging_onLine_status,
        battery_charging_status,
        navi_target_speed,
        navi_patrolling_route_name,
        navi_emergency_stop_switch_status,
        navi_warning_light_status,
        navi_speed,
        current_battery,
        atmosphere,
        pm25,
        co,
        co2,
        temperature,
        humidity,
        fog,
        tvoc,
        top_camera_connection_state,
        k8s_env_name,
        dt
    from event_remove_duplicate
    where rank=1;