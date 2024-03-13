set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;
set spark.executor.memory = 2G;
set mapreduce.job.queuename=root.prod;


with asr_tb as (
    SELECT  tdate, bucket,k8s_env_name,
            audio_lang,
            object_id,
            asr_vendor,
            asr_domain,
            is_noise,
            tenant_id,
            robot_type,
            audio_duration,
            audio_size
    from (
            SELECT to_date(event_time) as tdate,bucket,k8s_env_name,
                   audio_lang,
                   object_id,
                   asr_vendor,
                   asr_domain,
                   is_noise,
                   tenant_id,
                   robot_type,
                   audio_duration,
                   audio_size,
                   row_number() OVER (PARTITION BY bucket,k8s_env_name,audio_lang,object_id ORDER BY event_time DESC  ) as rnk
            from cdmdwd.dwd_asr_cmd_asr_i_d where dt='${e_dt_var}' and object_id !=''
        ) as tmp where tmp.rnk=1 and  audio_duration>0 and audio_size >0
)

insert overwrite table cdmdws.dws_bmd_asr_audio_statistic_i_d partition(dt)
select
    bucket,
    k8s_env_name,
    audio_lang,
    asr_vendor,
    asr_domain,
    is_noise,
    tenant_id,
    robot_type,
    audio_count,
    duration_count,
    size_count,
    tdate,
    '${e_dt_var}'   as dt
from (select tdate,
                      bucket,
                      k8s_env_name,
                      audio_lang,
                      asr_vendor,
                      asr_domain,
                      is_noise,
                      tenant_id,
                      robot_type,
                      count(1)                                   as audio_count,
                      sum(cast(audio_duration as bigint)) / 1000 as duration_count,
                      sum(cast(audio_size as bigint))            as size_count
               from asr_tb
               group by tdate, bucket, k8s_env_name,
                        audio_lang,
                        asr_vendor,
                        asr_domain,
                        is_noise,
                        tenant_id,
                        robot_type
              ) as tmp


