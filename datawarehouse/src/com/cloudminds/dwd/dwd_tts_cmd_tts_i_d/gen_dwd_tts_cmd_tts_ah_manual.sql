set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;
set mapreduce.input.fileinputformat.input.dir.recursive=true;
set hive.exec.max.dynamic.partitions.pernode=500;
set hive.merge.smallfiles.avgsize=128000000;
set hive.merge.size.per.task=128000000;
set mapreduce.job.queuename=root.users.liuhao;

with tts_es_data as (
    select
        *
    from hari.harix_etl_es
    where tdate < '${e_dt_var}' and tdate>= '${s_dt_var}' and k8s_svc_name = 'harix-skill-tts'
),
tts_data_tmp as (
    select
        json_extract as json_extract,
        if (trim(GET_JSON_OBJECT(json_extract,"$.json_extract.tenant_id")) is not null,
        nvl(trim(GET_JSON_OBJECT(json_extract,"$.json_extract.tenant_id")),''),
        nvl(trim(GET_JSON_OBJECT(json_extract,"$.json_extract.tenantId")),'')
        ) as tenant_id,
        if (trim(GET_JSON_OBJECT(json_extract,"$.json_extract.robot_type")) is not null,
        nvl(trim(GET_JSON_OBJECT(json_extract,"$.json_extract.robot_type")),''),
        nvl(trim(GET_JSON_OBJECT(json_extract,"$.json_extract.robotType")),'')
        ) as robot_type,
        if (trim(GET_JSON_OBJECT(json_extract,"$.json_extract.user_id")) is not null,
        nvl(trim(GET_JSON_OBJECT(json_extract,"$.json_extract.user_id")),''),
        nvl(trim(GET_JSON_OBJECT(json_extract,"$.json_extract.userId")),'')
        ) as user_id,
        if(trim(GET_JSON_OBJECT(json_extract,"$.json_extract.robot_id")) is not null,
        nvl(trim(GET_JSON_OBJECT(json_extract,"$.json_extract.robot_id")),''),
        nvl(trim(GET_JSON_OBJECT(json_extract,"$.json_extract.robotId")),'')
        ) as robot_id,
        nvl(trim(GET_JSON_OBJECT(json_extract,"$.json_extract.guid")),'') as guid,
        if(trim(GET_JSON_OBJECT(json_extract,"$.json_extract.service_code")) is not null,
        nvl(trim(GET_JSON_OBJECT(json_extract,"$.json_extract.service_code")),''),
        nvl(trim(GET_JSON_OBJECT(json_extract,"$.json_extract.serviceCode")),'')
        ) as service_code,
        if(trim(GET_JSON_OBJECT(json_extract,"$.json_extract.event_data.vendorDelay")) is not null,
        nvl(trim(GET_JSON_OBJECT(json_extract,"$.json_extract.event_data.vendorDelay")),-99999998),
        nvl(trim(GET_JSON_OBJECT(json_extract,"$.json_extract.ttsData.detail.vendorDelay")),-99999998)
        ) as vendor_delay,
        --nvl(trim(GET_JSON_OBJECT(json_extract,"$.json_extract.event_data.volume")),'') as volume,
        --nvl(trim(GET_JSON_OBJECT(json_extract,"$.json_extract.event_data.language")),'') as `language`,
        --nvl(trim(GET_JSON_OBJECT(json_extract,"$.json_extract.event_data.pitch")),'') as pitch,
        --nvl(trim(GET_JSON_OBJECT(json_extract,"$.json_extract.event_data.speed")),'') as speed,
        --nvl(trim(GET_JSON_OBJECT(json_extract,"$.json_extract.event_data.speaker")),'') as speaker,
        if(trim(GET_JSON_OBJECT(json_extract,"$.json_extract.event_data.gender")) is not null,
        nvl(trim(GET_JSON_OBJECT(json_extract,"$.json_extract.event_data.gender")),''),
        ''
        ) as gender,
        if(trim(GET_JSON_OBJECT(json_extract,"$.json_extract.event_data.is_cache")) is not null,
        nvl(trim(GET_JSON_OBJECT(json_extract,"$.json_extract.event_data.is_cache")),''),
        ''
        ) as is_cache,
        if(trim(GET_JSON_OBJECT(json_extract,"$.json_extract.event_data.audio")) is not null,
        nvl(trim(GET_JSON_OBJECT(json_extract,"$.json_extract.event_data.audio")),''),
        nvl(trim(GET_JSON_OBJECT(json_extract,"$.json_extract.ttsData.detail.fileUrl")),'')
        ) as audio,
        if(trim(GET_JSON_OBJECT(json_extract,"$.json_extract.event_data.text")) is not null,
        nvl(trim(GET_JSON_OBJECT(json_extract,"$.json_extract.event_data.text")),''),
        nvl(trim(GET_JSON_OBJECT(json_extract,"$.json_extract.ttsData.request.text")),'')
        ) as text,
        if(trim(GET_JSON_OBJECT(json_extract,"$.json_extract.event_data.tts_vendor")) is not null,
        nvl(trim(GET_JSON_OBJECT(json_extract,"$.json_extract.event_data.tts_vendor")),''),
        nvl(lower(get_json_object(json_extract,"$.json_extract.ttsData.detail.vendor")),regexp_extract(json_extract,'"ttsData":.+?"(baidu|cloudminds|google|huawei|microsoft)":.+',1))
        ) as tts_vendor,
        nvl(trim(GET_JSON_OBJECT(json_extract,"$.json_extract.version")),'') as version,
        if (GET_JSON_OBJECT(json_extract,"$.json_extract.event_time") is not null,
        replace(substring(trim(GET_JSON_OBJECT(json_extract,"$.json_extract.event_time")),0,23),'T',' '),
        replace(substring(trim(GET_JSON_OBJECT(json_extract,"$.json_extract.timestamp")),0,23),'T',' ')
        ) as event_time,
        if(trim(GET_JSON_OBJECT(json_extract,"$.json_extract.model_id")) is not null,
        nvl(trim(GET_JSON_OBJECT(json_extract,"$.json_extract.model_id")),''),
        ''
        ) as model_id,
        if(trim(GET_JSON_OBJECT(json_extract,"$.json_extract.root_guid")) is not null,
        nvl(trim(GET_JSON_OBJECT(json_extract,"$.json_extract.root_guid")),''),
        nvl(trim(GET_JSON_OBJECT(json_extract,"$.json_extract.rootGuid")),'')
        ) as root_guid,
        if(trim(GET_JSON_OBJECT(json_extract,"$.json_extract.event_type_id")) is not null,
        nvl(trim(GET_JSON_OBJECT(json_extract,"$.json_extract.event_type_id")),''),
        ''
        ) as event_type_id,
        nvl(trim(GET_JSON_OBJECT(json_extract,"$.k8s_env_name")),'') as k8s_env_name
    from tts_es_data
),
tts_data as (
    select
           tenant_id,
           robot_type,
           user_id,
           robot_id,
           guid,
           service_code,
           vendor_delay,
           gender,
           is_cache,
           audio,
           text,
           tts_vendor,
           model_id,
           root_guid,
           version,
           event_time,
           event_type_id,
           k8s_env_name,
           case
           when trim(GET_JSON_OBJECT(json_extract,"$.json_extract.event_data.tts_vendor")) is not null then nvl(trim(GET_JSON_OBJECT(json_extract,"$.json_extract.event_data.volume")),'')
           when tts_vendor='cloudminds' then nvl(get_json_object(json_extract,"$.json_extract.ttsData.request.cloudminds.volume"),'')
           when tts_vendor='baidu' then nvl(get_json_object(json_extract,"$.json_extract.ttsData.request.baidu.vol"),'')
           when tts_vendor='google' then nvl(get_json_object(json_extract,"$.json_extract.ttsData.request.google.volume"),'')
           when tts_vendor='huawei' then nvl(get_json_object(json_extract,"$.json_extract.ttsData.request.huawei.volume"),'')
           when tts_vendor='microsoft' then nvl(get_json_object(json_extract,"$.json_extract.ttsData.request.microsoft.volume"),'')
           end as volume,
           case
           when trim(GET_JSON_OBJECT(json_extract,"$.json_extract.event_data.tts_vendor")) is not null then nvl(trim(GET_JSON_OBJECT(json_extract,"$.json_extract.event_data.speed")),'')
           when tts_vendor='cloudminds' then nvl(get_json_object(json_extract,"$.json_extract.ttsData.request.cloudminds.speed"),'')
           when tts_vendor='baidu' then nvl(get_json_object(json_extract,"$.json_extract.ttsData.request.baidu.spd"),'')
           when tts_vendor='google' then nvl(get_json_object(json_extract,"$.json_extract.ttsData.request.google.speed"),'')
           when tts_vendor='huawei' then nvl(get_json_object(json_extract,"$.json_extract.ttsData.request.huawei.speechSpeed"),'')
           when tts_vendor='microsoft' then nvl(get_json_object(json_extract,"$.json_extract.ttsData.request.microsoft.speed"),'')
           end as speed,
           case
           when trim(GET_JSON_OBJECT(json_extract,"$.json_extract.event_data.tts_vendor")) is not null then nvl(trim(GET_JSON_OBJECT(json_extract,"$.json_extract.event_data.pitch")),'')
           when tts_vendor='cloudminds' then nvl(get_json_object(json_extract,"$.json_extract.ttsData.request.cloudminds.pitch"),'')
           when tts_vendor='baidu' then nvl(get_json_object(json_extract,"$.json_extract.ttsData.request.baidu.pit"),'')
           when tts_vendor='google' then nvl(get_json_object(json_extract,"$.json_extract.ttsData.request.google.pitch"),'')
           when tts_vendor='huawei' then nvl(get_json_object(json_extract,"$.json_extract.ttsData.request.huawei.pitchRate"),'')
           when tts_vendor='microsoft' then nvl(get_json_object(json_extract,"$.json_extract.ttsData.request.microsoft.pitch"),'')
           end as pitch,
           case
           when trim(GET_JSON_OBJECT(json_extract,"$.json_extract.event_data.tts_vendor")) is not null then nvl(trim(GET_JSON_OBJECT(json_extract,"$.json_extract.event_data.speaker")),'')
           when tts_vendor='cloudminds' then nvl(get_json_object(json_extract,"$.json_extract.ttsData.request.cloudminds.speaker"),'')
           when tts_vendor='baidu' then nvl(get_json_object(json_extract,"$.json_extract.ttsData.request.baidu.speaker"),'')
           when tts_vendor='google' then nvl(get_json_object(json_extract,"$.json_extract.ttsData.request.google.voiceName"),'')
           when tts_vendor='huawei' then nvl(get_json_object(json_extract,"$.json_extract.ttsData.request.huawei.voiceName"),'')
           when tts_vendor='microsoft' then nvl(get_json_object(json_extract,"$.json_extract.ttsData.request.microsoft.name"),'')
           end as speaker,
           case
           when trim(GET_JSON_OBJECT(json_extract,"$.json_extract.event_data.tts_vendor")) is not null then nvl(trim(GET_JSON_OBJECT(json_extract,"$.json_extract.event_data.language")),'')
           when tts_vendor='cloudminds' then nvl(get_json_object(json_extract,"$.json_extract.ttsData.request.cloudminds.lang"),'')
           when tts_vendor='baidu' then nvl(get_json_object(json_extract,"$.json_extract.ttsData.request.baidu.lan"),'')
           when tts_vendor='google' then nvl(get_json_object(json_extract,"$.json_extract.ttsData.request.google.languageCode"),'')
           when tts_vendor='huawei' then nvl(get_json_object(json_extract,"$.json_extract.ttsData.request.huawei.language"),'')
           when tts_vendor='microsoft' then nvl(get_json_object(json_extract,"$.json_extract.ttsData.request.microsoft.lang"),'')
           end as `language`
    from tts_data_tmp
),
tts_data_sort as (
    select
            tenant_id,
            robot_type,
            user_id,
            robot_id,
            guid,
            service_code,
            vendor_delay,
            volume,
            gender,
            speaker,
            is_cache,
            `language`,
            audio,
            pitch,
            text,
            tts_vendor,
            speed,
            model_id,
            root_guid,
            version,
            event_time,
            event_type_id,
            k8s_env_name,
            row_number() over (partition by audio,guid,k8s_env_name order by event_time desc) as rnk
    from tts_data
)
insert overwrite table cdmdwd.dwd_tts_cmd_tts_i_d partition(dt)
    select
        tenant_id,
        robot_type,
        user_id,
        robot_id,
        guid,
        service_code,
        vendor_delay,
        volume,
        gender,
        speaker,
        is_cache,
        `language`,
        audio,
        pitch,
        text,
        tts_vendor,
        speed,
        model_id,
        root_guid,
        version,
        event_time,
        event_type_id,
        k8s_env_name,
        substring(event_time,0,10) as dt
    from tts_data_sort where tts_data_sort.rnk = 1