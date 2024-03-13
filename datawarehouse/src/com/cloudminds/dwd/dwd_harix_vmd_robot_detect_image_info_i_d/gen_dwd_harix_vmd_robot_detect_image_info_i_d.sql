set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;
set mapreduce.job.queuename=root.users.liuhao;
---harix image
with harix_event_images_tmp as (
  select
      nvl(get_json_object(t1.json_extract,'$.json_extract.guid'),'') as guid,
      nvl(get_json_object(t1.json_extract,'$.json_extract.robotId'),'') as robot_id,
      nvl(get_json_object(t1.json_extract,'$.json_extract.robotType'),'') as robot_type_inner_name,
      nvl(get_json_object(t1.json_extract,'$.json_extract.tenantId'),'') as tenant_id,
      nvl(get_json_object(t1.json_extract,'$.json_extract.frData.request.type'),'') type,
      case get_json_object(t1.json_extract,'$.json_extract.frData.request.type') when 'DOOR_STATUS' then '开门状态识别'
                                                                              when 'FACE' then '人脸识别'
                                                                              when 'OCR' then '文字识别'
                                                                              when 'MONEY' then '钱币识别'
                                                                              when 'CAR_PLATE' then '车牌识别'
                                                                              when 'OBJECT' then '物体识别'
                                                                              when 'FACE_ATTR' then '人脸属性识别'
                                                                              when 'CAPTION' then '图像场景识别'
                                                                              when 'CLASSIFY' then '图像分类识别'
                                                                              when 'FALL' then '摔倒识别'
                                                                              when 'COMPARE' then '人证比对识别'
                                                                              when 'VENDING' then '货柜物品识别'
                                                                              when 'MASK' then '口罩识别'
                                                                              when 'FACE_INFRARED_TRACK' then '红外人脸检测轨迹识别'
                                                                              when 'VEHICLE' then '机动车类别识别'
                                                                              when 'GARBAGE_CAN' then '垃圾桶满溢识别'
                                                                              when 'TRASH' then '绿化带垃圾识别'
                                                                              when 'COUNT_CAN' then '垃圾桶位移识别'
                                                                              when 'CLIMB' then '翻越围栏识别'
                                                                              when 'CROWD' then '人群拥挤识别'
                                                                              when 'COUNT_VEHICLE' then '机动车拥挤识别'
                                                                              when 'UNIVERSAL' then '通用类型'
                                                                              when 'LASER_SCAN' then '激光点云数据识别'
                                                                              when 'RGBD' then '灰度图数据识别'
                                                                              when 'UNKNOWN' then '扩展类型'
                                                                              else '未知类型' end as type_name,
      regexp_replace(t2.image_url,'10.51.201.201','172.16.31.201') as image_url,
      concat('harix-skill-vision-',get_json_object(t1.json_extract,'$.json_extract.serviceCode')) as source,
      nvl(substr(regexp_replace(get_json_object(t1.json_extract,'$.json_extract.timestamp'),"T"," "),1,23),'') as event_time,
      nvl(get_json_object(t1.json_extract,'$.json_extract.frData'),'') as ext,
      t1.k8s_env_name as k8s_env_name,
      substr(get_json_object(t1.json_extract,'$.json_extract.timestamp'),1,10) as dt
  from hari.harix_etl_es t1 lateral view cdmudf.HarixVmdImageUrl(cast(get_json_object(json_extract,'$.json_extract.frData') as string)) t2
  where t1.tdate>='${s_dt_var}'
  and t1.tdate<='${e_dt_var}'
  and t1.k8s_svc_name='harix-skill-vision'
  and get_json_object(t1.json_extract,'$.json_extract.frData.request.type')!='VENDING_DYNAMIC'
),

---cv上报的人脸检测成功数据
face_detect_tmp as (
  select
    nvl(t1.`_id`,'') as event_id,
    nvl(t1.`_id`,'') as guid,
    nvl(t1.robot_id,'') as robot_id,
    nvl(t2.robot_type_inner_name,'') as robot_type_inner_name,
    nvl(t1.customer_code,'') as tenant_id,
    'FACE' as type,
    '人脸识别' as type_name,
    nvl(t1.detect_images,'') as image_url,
    concat('cv-',t1.source) as source,
    nvl(concat(from_unixtime(cast(t1.detect_time/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(t1.detect_time,11)),'') as event_time,
    concat('{"face_id":',t1.face_id,',"face_url":"',t1.face_url,'","is_mask":"',t1.is_mask,'","gender":"',t1.gender,'","detect_info":',t1.detect_info,'}') as ext,
    'bj-prod-232' as k8s_env_name,
     nvl(from_unixtime(cast(t1.detect_time/1000 as int),'yyyy-MM-dd'),'') as dt
  from (select * from cdmods.ods_cv_platform_db_c0017_face_detect_i_d where dt>='${s_dt_var}'and dt<='${e_dt_var}') t1 left join cdmdim.dim_pmd_robot_sh_d t2 on t1.robot_id=t2.robot_id
  where nvl(concat(from_unixtime(cast(t1.detect_time/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(t1.detect_time,11)),'')  between t2.start_time and t2.end_time
),
---cv上报的人脸检测失败数据
face_detect_unknown_tmp as (
  select
    nvl(t1.`_id`,'') as event_id,
    nvl(t1.`_id`,'') as guid,
    nvl(t1.robot_id,'') as robot_id,
    nvl(t2.robot_type_inner_name,'') as robot_type_inner_name,
    nvl(t1.customer_code,'') as tenant_id,
    'FACE' as type,
    '人脸识别' as type_name,
    nvl(t1.detect_images,'') as image_url,
    concat('cv-',t1.source) as source,
    nvl(concat(from_unixtime(cast(t1.detect_time/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(t1.detect_time,11)),'') as event_time,
    concat('{"is_mask":',t1.is_mask,',"detect_info":',t1.detect_info,'}') as ext,
    'bj-prod-232' as k8s_env_name,
     nvl(from_unixtime(cast(t1.detect_time/1000 as int),'yyyy-MM-dd'),'') as dt
  from (select * from cdmods.ods_cv_platform_db_c0017_face_detect_unknown_i_d where dt>='${s_dt_var}'and dt<='${e_dt_var}') t1 left join cdmdim.dim_pmd_robot_sh_d t2 on t1.robot_id=t2.robot_id
  where nvl(concat(from_unixtime(cast(t1.detect_time/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(t1.detect_time,11)),'')  between t2.start_time and t2.end_time
),
---合并数据
event_union as (
  select
     *,
     row_number() over (partition by event_id order by event_time asc) as rank
  from (
       select
           md5(concat(guid,image_url)) as event_id,
           guid,
           robot_id,
           robot_type_inner_name,
           tenant_id,
           type,
           type_name,
           image_url,
           source,
           event_time,
           ext,
           k8s_env_name,
           dt
       from harix_event_images_tmp
       union all
       select
           *
       from face_detect_tmp
       union all
       select
           *
       from face_detect_unknown_tmp
       union all
       select
           *
       from cdmdwd.dwd_harix_vmd_robot_detect_image_info_i_d
       where dt=date_sub('${s_dt_var}',1)
       ) t
)
insert overwrite table cdmdwd.dwd_harix_vmd_robot_detect_image_info_i_d partition(dt)
    select
        event_id,
        guid,
        robot_id,
        robot_type_inner_name,
        tenant_id,
        type,
        type_name,
        image_url,
        source,
        event_time,
        ext,
        k8s_env_name,
        dt
    from event_union
    where rank=1;