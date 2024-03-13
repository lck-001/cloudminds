-- harix_etl_es 与 harix_etl_es_221两个表合并
with es as (select *
	from hari.harix_etl_es
	where tdate='${t_dt_var}'
	union all
	select * from hari.harix_etl_es_221
	where tdate='${t_dt_var}'),
es_filter as (select json_extract,
  nvl(get_json_object(json_extract,"$.json_extract.timestamp"),'') as event_time,
  nvl(get_json_object(json_extract,"$.json_extract.tenantId"),'') as tenant_id,
  nvl(get_json_object(json_extract,"$.json_extract.robotId"),'') as robot_id,
  nvl(get_json_object(json_extract,"$.json_extract.userId"),'') as robot_account_id,
  nvl(get_json_object(json_extract,"$.json_extract.robotType"),'') as robot_type,
  nvl(get_json_object(json_extract,"$.json_extract.rootGuid"),'') as question_id,
  nvl(get_json_object(json_extract,"$.json_extract.serviceCode"),'') as service_code,
  nvl(get_json_object(json_extract,"$.json_extract.version"),'') as version,
  nvl(lower(get_json_object(json_extract,"$.json_extract.ttsData.detail.vendor")),regexp_extract(json_extract,'"ttsData":.+?"(baidu|cloudminds|google|huawei|microsoft)":.+',1)) as tts_vendor,
  nvl(get_json_object(json_extract,"$.json_extract.ttsData.detail.fileUrl"),'') as audio_url,
  nvl(get_json_object(json_extract,"$.json_extract.ttsData.request.text"),'') as tts_text,
  nvl(get_json_object(json_extract,"$.json_extract.ttsData.detail.vendorDelay"),'') as vendor_delay,
  nvl(get_json_object(json_extract,"$.json_extract.ttsData.detail.isCache"),'') as is_cache,
  k8s_env_name
  from es
  where nvl(get_json_object(json_extract,"$.json_extract.ttsData.detail.fileUrl"),'')!=''
),
-- 按照不同的vendor得到相应的值
es_all as (select
   regexp_extract(audio_url,'http[^\\/]*?:\\/\\/[^\\/]+?\\/(([^\\/]+?)\\/(.*?([^\\/]+?(\\.wav|\\.WAV|\\.mp3|\\.MP3|\\.pcm|\\.PCM))))',1) as object_id,
   regexp_extract(audio_url,'http[^\\/]*?:\\/\\/[^\\/]+?\\/(([^\\/]+?)\\/(.*?([^\\/]+?(\\.wav|\\.WAV|\\.mp3|\\.MP3|\\.pcm|\\.PCM))))',4) as voice_file_name,
   regexp_extract(audio_url,'http[^\\/]*?:\\/\\/[^\\/]+?\\/(([^\\/]+?)\\/(.*?([^\\/]+?(\\.wav|\\.WAV|\\.mp3|\\.MP3|\\.pcm|\\.PCM))))',2) as bucket,
   cast(concat(cast(unix_timestamp(SUBSTRING(REGEXP_REPLACE(event_time,'T',' '),0,19)) as string),substring(event_time,21,3)) as bigint) as event_time,
   tenant_id,
   robot_id,
   robot_account_id,
   robot_type,
   question_id,
   service_code,
   version,
   tts_vendor,
   case
   when tts_vendor='cloudminds' then nvl(get_json_object(json_extract,"$.json_extract.ttsData.request.cloudminds.volume"),'')
   when tts_vendor='baidu' then nvl(get_json_object(json_extract,"$.json_extract.ttsData.request.baidu.vol"),'')
   when tts_vendor='google' then nvl(get_json_object(json_extract,"$.json_extract.ttsData.request.google.volume"),'')
   when tts_vendor='huawei' then nvl(get_json_object(json_extract,"$.json_extract.ttsData.request.huawei.volume"),'')
   when tts_vendor='microsoft' then nvl(get_json_object(json_extract,"$.json_extract.ttsData.request.microsoft.volume"),'')
   end as volume,
   case
   when tts_vendor='cloudminds' then nvl(get_json_object(json_extract,"$.json_extract.ttsData.request.cloudminds.speed"),'')
   when tts_vendor='baidu' then nvl(get_json_object(json_extract,"$.json_extract.ttsData.request.baidu.spd"),'')
   when tts_vendor='google' then nvl(get_json_object(json_extract,"$.json_extract.ttsData.request.google.speed"),'')
   when tts_vendor='huawei' then nvl(get_json_object(json_extract,"$.json_extract.ttsData.request.huawei.speechSpeed"),'')
   when tts_vendor='microsoft' then nvl(get_json_object(json_extract,"$.json_extract.ttsData.request.microsoft.speed"),'')
   end as speed,
   case
   when tts_vendor='cloudminds' then nvl(get_json_object(json_extract,"$.json_extract.ttsData.request.cloudminds.pitch"),'')
   when tts_vendor='baidu' then nvl(get_json_object(json_extract,"$.json_extract.ttsData.request.baidu.pit"),'')
   when tts_vendor='google' then nvl(get_json_object(json_extract,"$.json_extract.ttsData.request.google.pitch"),'')
   when tts_vendor='huawei' then nvl(get_json_object(json_extract,"$.json_extract.ttsData.request.huawei.pitchRate"),'')
   when tts_vendor='microsoft' then nvl(get_json_object(json_extract,"$.json_extract.ttsData.request.microsoft.pitch"),'')
   end as pitch,
   case
   when tts_vendor='cloudminds' then nvl(get_json_object(json_extract,"$.json_extract.ttsData.request.cloudminds.speaker"),'')
   when tts_vendor='baidu' then nvl(get_json_object(json_extract,"$.json_extract.ttsData.request.baidu.speaker"),'')
   when tts_vendor='google' then nvl(get_json_object(json_extract,"$.json_extract.ttsData.request.google.voiceName"),'')
   when tts_vendor='huawei' then nvl(get_json_object(json_extract,"$.json_extract.ttsData.request.huawei.voiceName"),'')
   when tts_vendor='microsoft' then nvl(get_json_object(json_extract,"$.json_extract.ttsData.request.microsoft.name"),'')
   end as speaker,
   case
   when tts_vendor='cloudminds' then nvl(get_json_object(json_extract,"$.json_extract.ttsData.request.cloudminds.lang"),'')
   when tts_vendor='baidu' then nvl(get_json_object(json_extract,"$.json_extract.ttsData.request.baidu.lan"),'')
   when tts_vendor='google' then nvl(get_json_object(json_extract,"$.json_extract.ttsData.request.google.languageCode"),'')
   when tts_vendor='huawei' then nvl(get_json_object(json_extract,"$.json_extract.ttsData.request.huawei.language"),'')
   when tts_vendor='microsoft' then nvl(get_json_object(json_extract,"$.json_extract.ttsData.request.microsoft.lang"),'')
   end as lang,
   audio_url,
   tts_text,
   vendor_delay,
   is_cache,
   k8s_env_name from es_filter
),
-- 获取设备系统音量数据
all_volume as (select
  cast(concat(cast(unix_timestamp(event_time) as string),substring(event_time,21,3)) as bigint) as event_time_v,
	robot_id as robot_id_v,
	sys_volume
  from cdmdwd.dwd_cmd_sys_volume_i_d
  where dt<='${t_dt_var}'
  and sys_volume!=''
  ),
-- 将TTS事实表与设备系统音量表进行left join 按照设备id相等的条件
es_add_volume as (select *
  from es_all
	left outer join all_volume
	on es_all.robot_id=all_volume.robot_id_v
	),
-- 将不符合的数据置为null,得到时间差，正的最小值就是我们想要得到的那一列数据
result as (select
  object_id,
  voice_file_name,
  bucket,
  event_time,
  tenant_id,
  robot_id,
  robot_account_id,
  robot_type,
  question_id,
  service_code,
  version,
  tts_vendor,
  case when event_time_v=null
  then ''
  else if(event_time>=event_time_v,sys_volume,'')
  end as sys_volume,
  volume,
  speed,
  pitch,
  speaker,
  lang,
  audio_url,
  tts_text,
  vendor_delay,
  is_cache,
  k8s_env_name,
  case when event_time_v=null
  then null
  else if(event_time>=event_time_v,event_time-event_time_v,null)
  end as diff
  from es_add_volume),
-- 分区进行排序，记得null值排到最后(nulls last)
result_rank as (
  select row_number() over(partition by object_id,event_time order by diff nulls last) rn,*
  from result
  )

-- 将数据按天分区插入表中
insert overwrite table cdmdwd.dwd_tts_cmd_tts_i_d PARTITION(dt='${t_dt_var}') select
  object_id,
  voice_file_name,
  bucket,
  concat(from_unixtime(cast(substring(cast(event_time as string),1,10) AS BIGINT),'yyyy-MM-dd HH:mm:ss'),".",substring(cast(event_time as string),11,3)) as event_time,
  tenant_id,
  robot_id,
  robot_account_id,
  robot_type,
  question_id,
  service_code,
  version,
  tts_vendor,
  sys_volume,
  volume,
  speed,
  pitch,
  speaker,
  lang,
  audio_url,
  tts_text,
  vendor_delay,
  if(is_cache='',0,1) as is_cache,
  k8s_env_name
from result_rank where rn < 2;