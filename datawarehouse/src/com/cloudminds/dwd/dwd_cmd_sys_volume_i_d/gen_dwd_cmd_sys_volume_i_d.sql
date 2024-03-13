-- harix_etl_es 与 harix_etl_es_221两个表合并
with es as (select *
	from hari.harix_etl_es
	where tdate='${e_dt_var}'
	union all
	select * from hari.harix_etl_es_221
	where tdate='${e_dt_var}'),
hi as (select
  nvl(get_json_object(json_extract,"$.json_extract.serviceCode"),'') as service_code,
  nvl(get_json_object(json_extract,"$.json_extract.robotId"),'') as robot_id,
  nvl(get_json_object(json_extract,"$.json_extract.userId"),'') as robot_account_id,
  nvl(get_json_object(json_extract,"$.json_extract.robotType"),'') as robot_type,
  nvl(get_json_object(json_extract,"$.json_extract.tenantId"),'') as tenant_id,
  nvl(get_json_object(json_extract,"$.json_extract.switchData.request.body.param.tts.speakVolume"),'') as sys_volume,
  nvl(get_json_object(json_extract,"$.json_extract.timestamp"),'') as event_time,
  k8s_env_name
  from es
  where k8s_svc_name='harix-switch'
  and  nvl(get_json_object(json_extract,"$.json_extract.rodType"),'')='hiMsg'
),
roc as (select
  nvl(get_json_object(json_extract,"$.json_extract.serviceCode"),'') as service_code,
  nvl(get_json_object(json_extract,"$.json_extract.robotId"),'') as robot_id,
  nvl(get_json_object(json_extract,"$.json_extract.accountId"),'') as robot_account_id,
  nvl(get_json_object(json_extract,"$.json_extract.robotType"),'') as robot_type,
  nvl(get_json_object(json_extract,"$.json_extract.tenantId"),'') as tenant_id,
  nvl(get_json_object(json_extract,"$.json_extract.data.config.tts.speakVolume"),'') as sys_volume,
  nvl(get_json_object(json_extract,"$.json_extract.timestamp"),'') as event_time,
  k8s_env_name
  from hari.roc_operation_service
  where tdate='${e_dt_var}'),
all_data as (select *
	from hi
	union all
	select *
	from roc)

-- 将数据按天分区插入表中
insert overwrite table cdmdwd.dwd_cmd_sys_volume_i_d PARTITION(dt='${e_dt_var}') select
  service_code,
  robot_id,
  robot_account_id,
  robot_type,
  tenant_id,
  sys_volume,
  SUBSTRING(REGEXP_REPLACE(event_time,'T',' '),0,23) as event_time,
  k8s_env_name
from all_data