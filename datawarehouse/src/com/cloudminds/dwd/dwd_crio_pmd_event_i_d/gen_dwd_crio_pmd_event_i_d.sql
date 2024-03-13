set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;
set mapreduce.job.queuename=root.users.liuhao;
with event_tmp AS (
    SELECT 
        *,
        row_number() over(PARTITION BY 
            t.event_id,
            t.robot_type_inner_name,
            t.robot_id,
            t.robot_name,
            t.tenant_id,
            t.rcu_id,
            t.device_id,
            t.robot_type_id,
            t.type_id,
            t.type_name,
            t.event_time,
            t.level,
            t.details,
            t.ext_data,
            t.pic_url,
            t.location_info,
            t.create_time,
            t.read_flag,
            t.update_time,
            t.remark,
            t.msg_type_id,
            t.db_op
        ) as rnk
    from (
        SELECT 
            id as event_id, 
            nvl(name, '') as event_name, 
            nvl(robot_type, '') as robot_type_inner_name, 
            nvl(robot_code, '') as robot_id,
            nvl(robot_name, '') as robot_name,
            nvl(tenant_code, '') as tenant_id, 
            nvl(rcu_code, '') as rcu_id, 
            nvl(device_id, '') as device_id, 
            nvl(device_type, -99999998) as robot_type_id, 
            nvl(type_id, -99999998) as type_id, 
            nvl(type_name, '') as type_name,
            nvl(from_unixtime(cast(event_time as bigint), 'yyyy-MM-dd HH:mm:ss.SSS'), '') as event_time,
            nvl(level, -99999998) as level, 
            nvl(details, '') as details,
            nvl(ext_data, '') as ext_data,
            nvl(pic_url, '') as pic_url, 
            nvl(location_info, '') as location_info,
            nvl(from_unixtime(cast(create_time as bigint), 'yyyy-MM-dd HH:mm:ss.SSS'), '') as create_time,
            nvl(read_flag, -99999998) as read_flag,
            case read_flag WHEN 0 THEN decode(binary('未读'), 'utf-8') WHEN 1 THEN decode(binary('已读'), 'utf-8') ELSE decode(binary('未知'), 'utf-8') END as read_flag_name,
            nvl(from_unixtime(cast(update_time as bigint), 'yyyy-MM-dd HH:mm:ss.SSS'), '') as update_time,
            nvl(remark, '') as remark,
            nvl(msg_type_id, '') as msg_type_id,
            case lower(bigdata_method) WHEN 'insert' THEN 1 WHEN 'update' THEN 2 WHEN 'delete' THEN 3 ELSE -99999998 END as db_op,
            'bj-prod-232' as k8s_env_name,
            dt
        from cdmods.ods_crio_db_c0015_t_event_i_d 
        where 
            dt >= '${s_dt_var}'
        and 
            dt <= '${e_dt_var}'
        and
            device_code is not NULL
        and 
            device_code != ''
        -- AND (from_unixtime(cast(event_time as bigint), 'yyyy-MM-dd') >= '${e_dt_var}' or from_unixtime(cast(event_time as bigint), 'yyyy-MM-dd') <= date_add('${e_dt_var}', 1))
    ) t
)

insert overwrite table cdmdwd.dwd_crio_pmd_event_i_d partition(dt)
select 
    event_id,
    event_name,
    robot_type_inner_name,
    robot_id,
    robot_name,
    tenant_id,
    rcu_id,
    device_id,
    robot_type_id,
    type_id,
    type_name,
    event_time,
    level,
    details,
    ext_data,
    pic_url,
    location_info,
    create_time,
    read_flag,
    read_flag_name,
    update_time,
    remark,
    msg_type_id,
    db_op,
    k8s_env_name,
    dt
from event_tmp
where rnk = 1;
