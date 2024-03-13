---create by liuhao----
set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;
set mapreduce.input.fileinputformat.input.dir.recursive=true;
set hive.exec.max.dynamic.partitions.pernode=500;
set hive.merge.smallfiles.avgsize=128000000;
set hive.merge.size.per.task=128000000;
set mapreduce.job.queuename=root.users.liuhao;


with tts_data_tmp as (
    select
        nvl(trim(GET_JSON_OBJECT(json_extract,"$.tenant_id")),'') as tenant_id,
        nvl(trim(GET_JSON_OBJECT(json_extract,"$.robot_type")),'') as robot_type,
        nvl(trim(GET_JSON_OBJECT(json_extract,"$.user_id")),'') as user_id,
        nvl(trim(GET_JSON_OBJECT(json_extract,"$.robot_id")),'') as robot_id,
        nvl(trim(GET_JSON_OBJECT(json_extract,"$.guid")),'') as guid,
        nvl(trim(GET_JSON_OBJECT(json_extract,"$.service_code")),'') as service_code,
        nvl(trim(GET_JSON_OBJECT(json_extract,"$.event_data.vendorDelay")),-99999998) as vendor_delay,
        nvl(trim(GET_JSON_OBJECT(json_extract,"$.event_data.gender")),'') as gender,
        nvl(trim(GET_JSON_OBJECT(json_extract,"$.event_data.is_cache")),'') as is_cache,
        nvl(trim(GET_JSON_OBJECT(json_extract,"$.event_data.audio")),'') as audio,
        nvl(trim(GET_JSON_OBJECT(json_extract,"$.event_data.text")),'') as text,
        nvl(trim(GET_JSON_OBJECT(json_extract,"$.event_data.tts_vendor")),'') as tts_vendor,
        nvl(trim(GET_JSON_OBJECT(json_extract,"$.event_data.language")),'') as `language`,
        nvl(trim(GET_JSON_OBJECT(json_extract,"$.event_data.pitch")),'') as pitch,
        nvl(trim(GET_JSON_OBJECT(json_extract,"$.event_data.speaker")),'') as speaker,
        nvl(trim(GET_JSON_OBJECT(json_extract,"$.event_data.speed")),'') as speed,
        nvl(trim(GET_JSON_OBJECT(json_extract,"$.event_data.volume")),'') as volume,
        nvl(trim(GET_JSON_OBJECT(json_extract,"$.version")),'') as version,
        replace(substring(trim(GET_JSON_OBJECT(json_extract,"$.event_time")),0,23),'T',' ') as event_time,
        nvl(trim(GET_JSON_OBJECT(json_extract,"$.model_id")),'') as model_id,
        nvl(trim(GET_JSON_OBJECT(json_extract,"$.root_guid")),'') as root_guid,
        nvl(trim(GET_JSON_OBJECT(json_extract,"$.event_type_id")),'') as event_type_id,
        nvl(trim(GET_JSON_OBJECT(json_extract,"$.k8s_env_name")),'') as k8s_env_name
    from cdmods.ods_asr_event_04_i_d
    where event_id = '000001' and dt = '${e_dt_var}'
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
    from tts_data_tmp
)
insert overwrite table cdmdwd.dwd_tts_cmd_tts_i_d partition(dt='${e_dt_var}')
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
        k8s_env_name
    from tts_data_sort where tts_data_sort.rnk = 1