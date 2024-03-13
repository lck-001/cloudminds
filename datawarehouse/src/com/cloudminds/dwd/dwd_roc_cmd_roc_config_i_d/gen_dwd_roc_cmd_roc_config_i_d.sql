with report_config as (
  select
  rcu_code,
  nvl(get_json_object(asr,"$.language"),'') as asr_lang,
  nvl(get_json_object(asr,"$.type"),'') as asr_vendor,
  nvl(get_json_object(tts,"$.speakVolume"),'') as sys_volume,
  nvl(get_json_object(tts,"$.type"),'') as tts_vendor,
  nvl(get_json_object(tts,"$.speaker"),'') as tts_speaker,
  from_unixtime(cast(substring(cast(`timestamp` as string),1,10) AS BIGINT),'yyyy-MM-dd HH:mm:ss') as update_time
  from cdmods.ods_roc_db_c0005_reportmetrics_all_s_d
  where dt='${e_dt_var}'
),
robot_config as (
  select
  user_id,
  case
  when robot_type=1 then 'pepper'
  when robot_type=2 then 'meta'
  when robot_type=3 then 'patrol'
  else ''
  end as robot_type,
  regexp_extract(extension_json,'"lang\\\\\":\\\\\"(.+?)\\\\\"',1) as asr_lang,
  nvl(get_json_object(extension_json,"$.asr.type"),'') as asr_vendor,
  nvl(get_json_object(extension_json,"$.tts.volume"),'') as sys_volume,
  nvl(get_json_object(extension_json,"$.tts.vendor"),'') as tts_vendor,
  nvl(get_json_object(extension_json,"$.tts.speaker"),'') as tts_speaker,
  nvl(get_json_object(extension_json,"$.micArray.VUIArea"),'') as vui_area,
  nvl(get_json_object(extension_json,"$.micArray.pauseType"),'') as pause_type,
  update_time,
  extension_json
  from cdmods.ods_roc_db_c0002_t_current_robot_config_s_d
  where dt='${e_dt_var}'
),
library as (
  select
  library.id as library_id,
  user_library.user_id as user_id,
  if(tenant_library.tenant_code!='',tenant_library.tenant_code,library.tenant_code) as tenant_code,
  library.library_name as library_name,
  library.library_type as library_type,
  regexp_extract(library.library_value,'"(asrDomain|asr_domain)":[^"]*"(.+?)"',2) as asr_domain,
  regexp_extract(library.library_value,'"(smartvoice\\.agent\\.id)":[^"]*"(.+?)"',2) as agent_id,
  library.update_time as update_time,
  library.library_value as library_value
  from
  (select * from cdmods.ods_roc_db_c0002_t_library_s_d where dt='${e_dt_var}') library
  left join
  (select * from cdmods.ods_roc_db_c0002_t_tenant_library_s_d where dt='${e_dt_var}') tenant_library
  on tenant_library.library_id=library.id
  left join
  (select * from cdmods.ods_roc_db_c0002_t_user_library_s_d where dt='${e_dt_var}') user_library
  on library.id=user_library.library_id
),
user_rcu_robot as (
  select *
  from cdmods.ods_roc_db_c0002_t_user_rcu_robot_s_d where dt='${e_dt_var}' and status!=-1
),
all_config as (
  select
  if(robot_config.user_id!='',robot_config.user_id,library.user_id) as user_id,
  robot_config.robot_type as robot_type,
  robot_config.asr_lang as asr_lang,
  robot_config.asr_vendor as asr_vendor,
  robot_config.sys_volume as sys_volume,
  robot_config.tts_vendor as tts_vendor,
  robot_config.tts_speaker as tts_speaker,
  robot_config.vui_area as vui_area,
  robot_config.pause_type as pause_type,
  library.library_id as library_id,
  library.agent_id as agent_id,
  library.tenant_code as tenant_code,
  library.library_name as library_name,
  library.library_type as library_type,
  library.asr_domain as asr_domain,
  case when library.update_time>robot_config.update_time then library.update_time
  else robot_config.update_time
  end as update_time,
  library.library_value as library_value
  from robot_config
  full outer join
  library
  on robot_config.user_id=library.user_id
),
data_one as (
  select
  all_config.user_id as user_id,
  all_config.robot_type as robot_type,
  all_config.asr_lang as asr_lang,
  all_config.asr_vendor as asr_vendor,
  all_config.sys_volume as sys_volume,
  all_config.tts_vendor as tts_vendor,
  all_config.tts_speaker as tts_speaker,
  all_config.vui_area as vui_area,
  all_config.pause_type as pause_type,
  all_config.library_id as library_id,
  all_config.agent_id as agent_id,
  case when all_config.tenant_code!='' then all_config.tenant_code
  else user_rcu_robot.tenant_code
  end as tenant_code,
  all_config.library_name as library_name,
  all_config.library_type as library_type,
  all_config.library_value as library_value,
  all_config.asr_domain as asr_domain,
  user_rcu_robot.rcu_id as rcu_id,
  user_rcu_robot.robot_id as robot_id,
  user_rcu_robot.user_code as user_code,
  user_rcu_robot.rcu_code as rcu_code,
  user_rcu_robot.robot_code as robot_code,
  case when all_config.update_time>user_rcu_robot.update_time then all_config.update_time
  else user_rcu_robot.update_time
  end as update_time
  from all_config
  left join
  user_rcu_robot
  on all_config.user_id=user_rcu_robot.user_id and all_config.tenant_code=user_rcu_robot.tenant_code
),
data_two as (
  select
  report_config.rcu_code as rcu_code,
  report_config.asr_lang as asr_lang,
  report_config.asr_vendor as asr_vendor,
  report_config.sys_volume as sys_volume,
  report_config.tts_vendor as tts_vendor,
  report_config.tts_speaker as tts_speaker,
  user_rcu_robot.tenant_code as tenant_code,
  user_rcu_robot.rcu_id as rcu_id,
  user_rcu_robot.robot_id as robot_id,
  user_rcu_robot.user_code as user_code,
  -- user_rcu_robot.rcu_code as rcu_code,
  user_rcu_robot.robot_code as robot_code,
  case when report_config.update_time>user_rcu_robot.update_time then report_config.update_time
  else user_rcu_robot.update_time
  end as update_time
  from report_config
  left join
  user_rcu_robot
  on report_config.rcu_code=user_rcu_robot.rcu_code
),
data_all as (
  select
  if(data_one.rcu_code!='',data_one.rcu_code,data_two.rcu_code) as rcu_code,
  if(data_one.update_time>data_two.update_time,data_one.user_code,data_two.user_code) as user_code,
  if(data_one.update_time>data_two.update_time,data_one.robot_code,data_two.robot_code) as robot_code,
  if(data_one.update_time>data_two.update_time,data_one.asr_lang,data_two.asr_lang) as asr_lang,
  if(data_one.update_time>data_two.update_time,data_one.asr_vendor,data_two.asr_vendor) as asr_vendor,
  if(data_one.update_time>data_two.update_time,data_one.sys_volume,data_two.sys_volume) as sys_volume,
  if(data_one.update_time>data_two.update_time,data_one.tts_vendor,data_two.tts_vendor) as tts_vendor,
  if(data_one.update_time>data_two.update_time,data_one.tts_speaker,data_two.tts_speaker) as tts_speaker,
  if(data_one.update_time>data_two.update_time,data_one.tenant_code,data_two.tenant_code) as tenant_code,
  data_one.vui_area as vui_area,
  data_one.pause_type as pause_type,
  data_one.asr_domain as asr_domain,
  data_one.agent_id as agent_id,
  data_one.library_id as library_id,
  data_one.library_name as library_name,
  data_one.library_type as library_type,
  data_one.library_value as library_value,
  if(data_one.update_time>data_two.update_time,data_one.update_time,data_two.update_time) as update_time
  from data_two
  full outer join
  data_one
  on data_one.rcu_code=data_two.rcu_code
),
result as (
  select row_number() over(partition by rcu_code order by update_time desc nulls last) rn,*
  from data_all
  where rcu_code!=''
)

-- 将数据按天分区插入表中
insert overwrite table cdmdwd.dwd_roc_cmd_roc_config_i_d PARTITION(dt='${e_dt_var}') select
	rcu_code,
	user_code,
	robot_code,
	asr_lang,
	asr_vendor,
	sys_volume,
	tts_vendor,
	tts_speaker,
	tenant_code,
	vui_area,
	pause_type,
	asr_domain,
	agent_id,
	library_id,
	library_name,
	library_type,
	library_value,
	update_time
	from result where rn < 2