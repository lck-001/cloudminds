set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;
set hive.exec.max.dynamic.partitions.pernode = 60000;
set mapreduce.job.queuename=root.users.liuhao;

---harix video
with harix_event_video_tmp as (
  select
      md5(concat(nvl(get_json_object(t1.json_extract,'$.json_extract.guid'),''),t2.image_url)) as event_id,
      nvl(get_json_object(t1.json_extract,'$.json_extract.guid'),'') as guid,
      nvl(get_json_object(t1.json_extract,'$.json_extract.robotId'),'') as robot_id,
      nvl(get_json_object(t1.json_extract,'$.json_extract.robotType'),'') as robot_type_inner_name,
      nvl(get_json_object(t1.json_extract,'$.json_extract.tenantId'),'') as tenant_id,
      'VENDING_DYNAMIC' as type,
      '美的动态货柜识别' as type_name,
      t2.image_url as video_url,
      concat('harix-skill-vision-',get_json_object(t1.json_extract,'$.json_extract.serviceCode')) as source,
      nvl(substr(regexp_replace(get_json_object(t1.json_extract,'$.json_extract.timestamp'),"T"," "),1,23),'') as event_time,
      nvl(get_json_object(t1.json_extract,'$.json_extract.frData'),'') as ext,
      t1.k8s_env_name as k8s_env_name,
      substr(get_json_object(t1.json_extract,'$.json_extract.timestamp'),1,10) as dt
  from hari.harix_etl_es t1
  lateral view cdmudf.HarixVmdImageUrl(cast(get_json_object(json_extract,'$.json_extract.frData') as string)) t2
  where t1.tdate>='${s_dt_var}'
  and t1.tdate<='${e_dt_var}'
  and t1.k8s_svc_name='harix-skill-vision'
  and get_json_object(t1.json_extract,'$.json_extract.frData.request.type')='VENDING_DYNAMIC'
),
---vending_log带上租户等信息的数据
crss_vending_log_tmp as (
  select
    nvl(cast(t1.id as string),'') as guid,
    nvl(t2.robot_code,'') as robot_id,
    nvl(t2.robot_type,'') as robot_type_inner_name,
    nvl(t2.tenant_code,'') as tenant_id,
    'VENDING_DYNAMIC' as type,
    '美的动态货柜识别' as type_name,
    t1.ext_data as video_urls,
    'crss-vending' as source,
    t1.ext_data as ext,
    'bj-prod-232' as k8s_env_name
  from (
    select
        *
    from crss.dwd_vending_log
    where dt>='${s_dt_var}'and dt<='${e_dt_var}' and create_time>concat(date_sub('${s_dt_var}',1),' 00:00:00')
    and nvl(ext_data,'')!='') t1
    left join (select * from cdmtmp.tmp_vending_s_d_tanqiong_20780302 where dt='${e_dt_var}') t2 on t1.vending_id=t2.id
  where nvl(t2.robot_code,'')!=''
),
---将带上租户等信息的数据做一部分去重deal
crss_vending_log_deal as (
  select
    md5(concat(t1.guid,t2.video_url)) as event_id,
    t1.guid,
    t1.robot_id,
    t1.robot_type_inner_name,
    t1.tenant_id,
    t1.type,
    t1.type_name,
    t2.video_url,
    t1.source,
    nvl(concat(from_unixtime(cast(t2.report_time/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(t2.report_time,11)),'') as event_time,
    t1.ext,
    t1.k8s_env_name,
    nvl(from_unixtime(cast(t2.report_time/1000 as int),'yyyy-MM-dd'),'') as dt
  from crss_vending_log_tmp t1
  lateral view cdmudf.VendingVideoUrl(video_urls) t2),

---合并数据
event_union as (
  select
     *,
     row_number() over (partition by event_id order by event_time asc) as rank
  from (
      select
          *
      from harix_event_video_tmp
      union all
      select
          *
      from crss_vending_log_deal
      union all
      select
          *
      from cdmdwd.dwd_harix_vmd_robot_detect_video_info_i_d
      where (dt=date_sub('${s_dt_var}',1) or dt='${s_dt_var}')) t
)
insert overwrite table cdmdwd.dwd_harix_vmd_robot_detect_video_info_i_d partition(dt)
    select
        event_id,
        guid,
        robot_id,
        robot_type_inner_name,
        tenant_id,
        type,
        type_name,
        video_url,
        source,
        event_time,
        ext,
        k8s_env_name,
        dt
    from event_union
    where rank=1;