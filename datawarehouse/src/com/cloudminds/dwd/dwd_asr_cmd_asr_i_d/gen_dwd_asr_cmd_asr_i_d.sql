-- ASR音频事实表,抽取harix_etl_es中的ASR音频信息,每日进行调度。
-- 将两个表的数据合并
with es as (select * from hari.harix_etl_es where tdate="${e_dt_var}" union all select * from hari.harix_etl_es_221 where tdate="${e_dt_var}"),
es_filter as (select
  nvl(get_json_object(json_extract,"$.json_extract.asrData.detail.wavPath"),'') as audio_file_url,
  nvl(get_json_object(json_extract,"$.json_extract.asrData.request.data.duration"),-99999998) as audio_duration,
  nvl(get_json_object(json_extract,"$.json_extract.asrData.detail.wavLength"),-99999998) as audio_size,
  nvl(get_json_object(json_extract,"$.json_extract.asrData.request.data.rate"),-99999998) as audio_rate,
  nvl(get_json_object(json_extract,"$.json_extract.asrData.request.data.language"),'') as audio_lang,
  nvl(get_json_object(json_extract,"$.json_extract.asrData.request.data.format"),'') as audio_format,
  nvl(get_json_object(json_extract,"$.json_extract.asrData.request.data.channel"),-99999998) as audio_channel,
  nvl(get_json_object(json_extract,"$.json_extract.asrData.request.type"),'') as asr_type,
  nvl(get_json_object(json_extract,"$.json_extract.asrData.request.data.vendor"),'') as asr_vendor,
  nvl(get_json_object(json_extract,"$.json_extract.asrData.request.version"),'') as asr_service_version,
  nvl(get_json_object(json_extract,"$.json_extract.asrData.request.option.tstAsrDomain"),'') as asr_domain,
  nvl(get_json_object(json_extract,"$.json_extract.asrData.detail.recognizedText"),'') as asr_text,
  nvl(get_json_object(json_extract,"$.json_extract.asrData.response.questionId"),'') as question_id,
  nvl(get_json_object(json_extract,"$.json_extract.asrData.response.isNoise"),'') as is_noise,
  nvl(get_json_object(json_extract,"$.json_extract.asrData.request.serviceCode"),'') as service_code,
  nvl(get_json_object(json_extract,"$.json_extract.robotId"),'') as robot_id,
  nvl(get_json_object(json_extract,"$.json_extract.userId"),'') as robot_account_id,
  nvl(get_json_object(json_extract,"$.json_extract.robotType"),'') as robot_type,
  nvl(get_json_object(json_extract,"$.json_extract.tenantId"),'') as tenant_id,
  k8s_env_name,
  nvl(get_json_object(json_extract,"$.json_extract.asrData.request.localParams.AgentId"),'') as agent_id,
  nvl(get_json_object(json_extract,"$.json_extract.timestamp"),'') as event_time
  from es where
  k8s_svc_name='smartvoice-asrctrl'
  and nvl(get_json_object(json_extract,"$.json_extract.asrData"),'')!=''
  and nvl(get_json_object(json_extract,"$.json_extract.rodType"),'')='asrRecognize'
),
es_all as ( select
  regexp_extract(audio_file_url,'http[^\\/]*?:\\/\\/[^\\/]+?\\/(([^\\/]+?)\\/(.*?([^\\/]+?(\\.wav|\\.WAV|\\.mp3|\\.MP3|\\.pcm|\\.PCM))))',1) as object_id,
  regexp_extract(audio_file_url,'http[^\\/]*?:\\/\\/[^\\/]+?\\/(([^\\/]+?)\\/(.*?([^\\/]+?(\\.wav|\\.WAV|\\.mp3|\\.MP3|\\.pcm|\\.PCM))))',4) as voice_file_name,
  regexp_extract(audio_file_url,'http[^\\/]*?:\\/\\/[^\\/]+?\\/(([^\\/]+?)\\/(.*?([^\\/]+?(\\.wav|\\.WAV|\\.mp3|\\.MP3|\\.pcm|\\.PCM))))',2) as bucket,
  audio_file_url,
  audio_duration,
  audio_size,
  audio_rate,
  audio_lang,
  audio_format,
  audio_channel,
  asr_type,
  asr_vendor,
  asr_service_version,
  asr_domain,
  asr_text,
  question_id,
  if(is_noise='true',1,0) as is_noise,
  service_code,
  robot_id,
  robot_account_id,
  robot_type,
  tenant_id,
  k8s_env_name,
  agent_id,
  event_time from es_filter
),
-- 按音频链接进行聚合得到k8s_env_name
es_all_group_env as (select
    max(k8s_env_name) as k8s_env_name,
    object_id
    from es_all group by object_id),
-- 按音频链接进行聚合得到除k8s_env_name以外的数据
es_all_group_other as (select
    max(audio_duration) as audio_duration,
    max(audio_size) as audio_size,
    max(audio_rate) as audio_rate,
    max(audio_lang) as audio_lang,
    max(audio_format) as audio_format,
    max(audio_channel) as audio_channel,
    max(asr_type) as asr_type,
    max(asr_service_version) as asr_service_version,
    max(question_id) as question_id,
    max(is_noise) as is_noise,
    max(service_code) as service_code,
    max(event_time) as event_time,
    max(agent_id) as agent_id,
    max(asr_domain) as asr_domain,
    max(robot_type) as robot_type,
    max(robot_id) as robot_id,
    max(robot_account_id) as robot_account_id,
    max(tenant_id) as tenant_id,
    max(asr_vendor) as asr_vendor,
    max(audio_file_url) as audio_file_url,
    max(asr_text) as asr_text,
    object_id as object_id,
    max(voice_file_name) as voice_file_name,
    max(bucket) as bucket
    from es_all group by object_id),
-- 将上面两张聚合后的表进行内联得到结果
es_result as (select
    b1.audio_duration as audio_duration,
    b1.audio_size as audio_size,
    b1.audio_rate as audio_rate,
    b1.audio_lang as audio_lang,
    b1.audio_format as audio_format,
    b1.audio_channel as audio_channel,
    b1.asr_type as asr_type,
    b1.asr_service_version as asr_service_version,
    b1.question_id as question_id,
    b1.is_noise as is_noise,
    b1.service_code as service_code,
    case when length(b1.event_time) = 10 then nvl(concat(from_unixtime( cast(b1.event_time as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
      when length(b1.event_time) = 13 then nvl(concat(from_unixtime(cast(substring(b1.event_time,1,10) AS BIGINT),'yyyy-MM-dd HH:mm:ss'),".",substring(b1.event_time,11,3)),'')
      when b1.event_time regexp '.+T.+\\.\\d{3,}.+08:00$' then nvl(substring(REGEXP_REPLACE(b1.event_time,'T',' '),0,23),'')
      when b1.event_time regexp '.+T.+[^.].+08:00$' then nvl(concat(substring(REGEXP_REPLACE(b1.event_time,'T',' '),0,19),'.000'),'')
      when b1.event_time regexp '.+T.+\\.\\d{3,}$' then nvl(from_utc_timestamp(substring(REGEXP_REPLACE(b1.event_time,'T',' '),0,23),'PRC'),'')
      when b1.event_time regexp '.+T.+[^.]+' then nvl(from_utc_timestamp(substring(REGEXP_REPLACE(b1.event_time,'T',' '),0,19),'PRC'),'')
      when b1.event_time regexp '.+[^T].+\\.\\d{3,}$+' then nvl(b1.event_time,'')
      when b1.event_time regexp '.+[^T].+[^.].+' then nvl(concat(b1.event_time,'.000'),'')
      else nvl(b1.event_time,'')
      end as event_time,
    b1.agent_id as agent_id,
    b1.asr_domain as asr_domain,
    b1.robot_type as robot_type,
    b1.robot_id as robot_id,
    b1.robot_account_id as robot_account_id,
    b1.tenant_id as tenant_id,
    b1.asr_vendor as asr_vendor,
    b1.asr_text as asr_text,
    b1.object_id as object_id,
    b1.voice_file_name as voice_file_name,
    b1.bucket as bucket,
    b2.k8s_env_name as k8s_env_name,
    b1.audio_file_url as audio_file_url
    from es_all_group_other b1 inner join es_all_group_env b2 on b1.object_id=b2.object_id
  )

-- 将数据按天分区插入表中
insert overwrite table cdmdwd.dwd_asr_cmd_asr_i_d PARTITION(dt="${e_dt_var}") select
  object_id,
  voice_file_name,
  bucket,
  audio_file_url,
  audio_duration,
  audio_size,
  audio_rate,
  audio_lang,
  audio_format,
  audio_channel,
  asr_type,
  asr_vendor,
  asr_service_version,
  asr_domain,
  asr_text,
  question_id,
  is_noise,
  service_code,
  robot_id,
  robot_account_id,
  robot_type,
  tenant_id,
  k8s_env_name,
  agent_id,
  event_time
from es_result