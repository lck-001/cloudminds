set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;
set mapreduce.job.queuename=root.prod;

insert overwrite table cdmdwd.dwd_crio_vmd_ceph_event_i_d partition(dt)
    select
        *
    from (select
             nvl(get_json_object(json_extract,'$.object_id'),'') as object_id,
             nvl(get_json_object(json_extract,'$.s3_user_id'),'') as s3_user_id,
             nvl(get_json_object(json_extract,'$.display_name'),'') as display_name,
             cast(nvl(get_json_object(json_extract,'$.file_size'),0) as bigint) as file_size,
             nvl(get_json_object(json_extract,'$.file_type'),'')  as file_type,
             0 as compress_file_size,
             nvl(get_json_object(json_extract,'$.bucket'),'') as bucket,
             '' as md5,
             '' as user_data,
             CONCAT(get_json_object(json_extract,'$.event_time'),".000") as event_time,
             nvl(get_json_object(json_extract,'$.suspended'),'')  as suspended,
             nvl(get_json_object(json_extract,'$.access_key'),'')  as access_key,
             nvl(get_json_object(json_extract,'$.max_buckets'),'')  as max_buckets,
             nvl(get_json_object(json_extract,'$.placement_tags'),'')  as placement_tags,
             nvl(get_json_object(json_extract,'$.default_placement'),'')  as default_placement,
             nvl(get_json_object(json_extract,'$.default_storage_class'),'')  as default_storage_class,
             nvl(get_json_object(json_extract,'$.op_mask'),'')  as op_mask,
             'bj-prod-232' as k8s_env_name,
             nvl(substring(get_json_object(json_extract,'$.event_time'),1,10),'') as dt
           from cdmods.ods_crio_event_ceph_i_d
           where dt>='${s_dt_var}'
           and dt<='${e_dt_var}'
           union all
           select
             *
           from cdmdwd.dwd_crio_vmd_ceph_event_i_d
           where dt=date_sub('${s_dt_var}', 1)) a;