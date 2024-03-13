set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;
set mapreduce.job.queuename=root.users.liuhao;

with data as (
     select
        *,
        row_number() over(partition by id ORDER BY update_time desc) as rnk
     from cdmods.ods_crio_db_c0012_vending_i_d where dt>='${s_dt_var}' and dt<='${e_dt_var}')

insert overwrite table cdmtmp.tmp_vending_s_d_tanqiong_20780302 partition(dt)
    select
       id,
       tenant_id,
       tenant_code,
       branch_id,
       region_id,
       name,
       service_code,
       rcu_code,
       robot_type,
       robot_code,
       roc_user_code,
       online_flag,
       hari_service_flag,
       longitude,
       latitude,
       status,
       model,
       location,
       scene,
       door_status,
       lock_status,
       door2_status,
       lock2_status,
       firmware_version,
       software_version,
       deliver_person_id,
       stockin_person_id,
       create_time,
       update_time,
       version,
       layout_id,
       floor_sku,
       shfp_flag,
       door_post_url,
       shfp_machine_sn,
       'bj-prod-232' as k8s_env_name,
       '${e_dt_var}' as dt
    from data where rnk=1;