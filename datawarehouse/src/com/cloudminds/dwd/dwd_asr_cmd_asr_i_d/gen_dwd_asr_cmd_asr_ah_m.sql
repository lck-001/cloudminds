-- ASR音频事实表,抽取harix_etl_es中的ASR音频信息，从2019-01-14开始的历史数据，只运行一次。
-- 将两个表的数据合并
with es as (select * from hari.harix_etl_es where tdate="${e_dt_var}" union all select * from hari.harix_etl_es_221 where tdate="${e_dt_var}"),
es_regex as (select tdate,k8s_env_name,
    regexp_extract(json_extract,'"asrData":.+?"duration":([^"]+?)(,|})',1) as audio_duration,
    regexp_extract(json_extract,'"asrData":.+?"wavLength":([^"]+?)(,|})',1) as audio_size,
    regexp_extract(json_extract,'"asrData":.+?"rate":([^"]+?)(,|})',1) as audio_rate,
    regexp_extract(json_extract,'"asrData":.+?"language":"([^"]+?)"',1) as audio_lang,
    regexp_extract(json_extract,'"asrData":.+?"format":"([^"]+?)"',1) as audio_format,
    regexp_extract(json_extract,'"asrData":.+?"channel":([^"]+?)(,|})',1) as audio_channel,
    regexp_extract(json_extract,'"asrData":.+?"type":"([^"]+?)"',1) as asr_type,
    regexp_extract(json_extract,'"asrData":.+?"version":"([^"]+?)"',1) as asr_service_version,
    regexp_extract(json_extract,'"asrData":.+?"questionId":"([^"]+?)"',1) as question_id,
    regexp_extract(json_extract,'"asrData":.+?"isNoise":("|)([^"]+?)("|)(,|})',2) as is_noise,
    regexp_extract(json_extract,'"asrData":.+?"serviceCode":"([^"]+?)"',1) as service_code,
    regexp_extract(json_extract,'"asrData":.+?"timestamp":("|)([^"]+?)("|)(,|})',2) as event_time,
    regexp_extract(json_extract,'"asrData":.+?"AgentId":("|)([^"]+?)("|)(,|})',2) as agent_id,
    regexp_extract(json_extract,'"asrData":.+?"tstAsrDomain":"([^"]+?)"',1) as asr_domain,
    regexp_extract(json_extract,'"robotType":"([^"]+?)"',1) as robot_type,
    regexp_extract(json_extract,'"robotId":"([^"]+?)"',1) as robot_id,
    regexp_extract(json_extract,'"userId":"([^"]+?)"',1) as robot_account_id,
    regexp_extract(json_extract,'"vendor":"([^"]+?)"',1) as asr_vendor,
    regexp_extract(json_extract,'("asrData":)',1) as asr_data,
    regexp_extract(json_extract,'"tenantId":"([^"]+?)"',1) as tenant_id,
    regexp_extract(json_extract,'"detail":.+"recognizedText":"([^"]+?)"',1) as WavTextContent,
    regexp_extract(json_extract,'(\\\\\|)"(Question|question|detailMessage)(\\\\\|)":.+?(\\\\\|)"text(\\\\\|)":(\\\\\|)"([^"]+?)(\\\\\|)"',7) as audioTextContent,
    regexp_extract(json_extract,'"asrData":.+?"RecognizedText":"([^"]+?)"',1) as asrTextText,
    regexp_extract(json_extract,'(\\\\\|)"text(\\\\\|)":(\\\\\|)"([^"]+?)(\\\\\|)"',4) as textContent,
    regexp_extract(json_extract,'"recognizedText":"([^"]+?)"',1) as recognizedText,
    regexp_extract(json_extract,'"detail":.+?"wavPath":.*?(http[^"]+?)"',1) as wavUrlString,
    regexp_extract(json_extract,'"(Question|question|detailMessage)":.+?"audio":.*?(http[^"]+?)"',2) as audioUrlString,
    regexp_extract(json_extract,'"(Question|question|detailMessage)":.+?"pcmUrl":.*?(http[^"]+?)"',2) as pcmUrlString,
    regexp_extract(json_extract,'"asrData":.+?"[Ff]ileUrl":.*?(http[^"]+?)"',1) as fileUrlAsrString,
    regexp_extract(json_extract,'(\\\\\|)"([^"]+?)(\\\\\|)":((\\\\\|)"=HYPERLINK\\(|)(\\\\\|)"(http[^"]+?(\\.wav|\\.WAV|\\.mp3|\\.MP3|\\.pcm|\\.PCM))',2) as attr_name,
    regexp_extract(json_extract,'"(http[^"]+?(\\.wav|\\.WAV|\\.mp3|\\.MP3|\\.pcm|\\.PCM)[^"]*?)(\\\\\|)"',1) as otherUrlString
    from es),
es_filter as (select
  tdate,
  audio_duration,
  audio_size,
  audio_rate,
  audio_lang,
  audio_format,
  audio_channel,
  asr_type,
  asr_service_version,
  question_id,
  --is_noise,
  service_code,
  event_time,
  agent_id,
  asr_domain,
  k8s_env_name,
  robot_type,
  robot_id,
  robot_account_id,
  asr_vendor,
  tenant_id,
  attr_name,
  case
  when is_noise=="true" then 1
  else 0
  end as is_noise,
  case
    when WavTextContent!="" then WavTextContent
    when audioTextContent!="" then audioTextContent
    when asrTextText!="" then asrTextText
    when textContent!="" then textContent
    when recognizedText!="" then recognizedText
    else ""
  end as asr_text,
  case
    when wavUrlString!="" then wavUrlString
    when fileUrlAsrString!="" then fileUrlAsrString
    when audioUrlString!="" then audioUrlString
    when pcmUrlString!="" then pcmUrlString
    else otherUrlString
  end as audio_file_url
  from es_regex where attr_name!="url" and attr_name!="StringValue" and attr_name!="" and asr_data!=""),
es_all as ( select
  tdate,
  audio_duration,
  audio_size,
  audio_rate,
  audio_lang,
  audio_format,
  audio_channel,
  asr_type,
  asr_service_version,
  question_id,
  is_noise,
  service_code,
  event_time,
  agent_id,
  asr_domain,
  k8s_env_name,
  robot_type,
  robot_id,
  robot_account_id,
  asr_vendor,
  tenant_id,
  asr_text,
  regexp_extract(audio_file_url,'http[^\\/]*?:\\/\\/[^\\/]+?\\/(([^\\/]+?)\\/(.*?([^\\/]+?(\\.wav|\\.WAV|\\.mp3|\\.MP3|\\.pcm|\\.PCM))))',1) as object_id,
  regexp_extract(audio_file_url,'http[^\\/]*?:\\/\\/[^\\/]+?\\/(([^\\/]+?)\\/(.*?([^\\/]+?(\\.wav|\\.WAV|\\.mp3|\\.MP3|\\.pcm|\\.PCM))))',4) as voice_file_name,
  regexp_extract(audio_file_url,'http[^\\/]*?:\\/\\/[^\\/]+?\\/(([^\\/]+?)\\/(.*?([^\\/]+?(\\.wav|\\.WAV|\\.mp3|\\.MP3|\\.pcm|\\.PCM))))',2) as bucket,
  audio_file_url
  from es_filter
),
-- 按音频链接进行聚合得到k8s_env_name
es_all_group_env as (select
    max(k8s_env_name) as k8s_env_name,
    object_id
    from es_all group by object_id),
-- 按音频链接进行聚合得到除k8s_env_name以外的数据
es_all_group_other as (select
    collect_set(tdate)[0] as tdate,
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
    b1.tdate as tdate,
    if(b1.audio_duration='',-99999998,b1.audio_duration) as audio_duration,
    if(b1.audio_size='',-99999998,b1.audio_size) as audio_size,
    if(b1.audio_rate='',-99999998,b1.audio_rate) as audio_rate,
    b1.audio_lang as audio_lang,
    b1.audio_format as audio_format,
    if(b1.audio_channel='',-99999998,b1.audio_channel) as audio_channel,
    b1.asr_type as asr_type,
    b1.asr_service_version as asr_service_version,
    b1.question_id as question_id,
    b1.is_noise as is_noise,
    b1.service_code as service_code,
    concat(from_unixtime(cast(substring(b1.event_time,1,10) AS BIGINT),'yyyy-MM-dd HH:mm:ss'),".",substring(b1.event_time,11,3)) as event_time,
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