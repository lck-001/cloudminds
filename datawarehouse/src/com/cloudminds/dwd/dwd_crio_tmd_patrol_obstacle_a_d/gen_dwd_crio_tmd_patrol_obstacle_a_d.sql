set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;
set mapreduce.job.queuename=root.users.liuhao;

with patrol_log as (
    select
        oid,
        robot_id,
        project_code,
        project_name,
        description,
        event_type,
        tenant_id,
        pub_time,
        uuid,
        event_time
    from (
         select
             `_id` as oid,
             trim(robotid) as robot_id,
             trim(projectCode) as project_code,
             trim(projectName) as project_name,
             regexp_replace(nvl(`describe`,''), '\r|\n|\t', '') as description,
             trim(title) as event_type,
             trim(tenant) as tenant_id,
             pubtime as pub_time,
             trim(uuid) as uuid,
             case when length(pubtime) = 10 then nvl(concat(from_unixtime( cast(pubtime as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
                  when length(pubtime) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(pubtime,'T',' '),0,23),'')
                  else nvl(concat(from_unixtime( cast(pubtime/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(pubtime,11,3)),'')
                 end as event_time
         from cdmods.ods_dams_db_c0020_partrollog_i_d
    ) t where event_type in ('obstacle','avoidance')
),
obstacle_table as (
    select
        oid,
        case when uuid is not null and trim(uuid) != '' then uuid
             else oid
        end as event_id,
        nvl(robot_id,'') as robot_id,
        nvl(tenant_id,'') as tenant_id,
        case when nvl(project_code,'') != '' then project_code
             else nvl(project_code_desc,'')
        end as project_code,
        case when nvl(project_name,'') != '' then project_name
             else nvl(project_name_desc,'')
        end as project_name,
        nvl(event_type,'') as event_type,
        '启动巡逻' as event_type_name,
        nvl(description,'') as description,
        nvl(programme_id,'') as programme_id,
        nvl(programme_name,'') as programme_name,
        nvl(avoidance_time,'') as avoidance_time,
        nvl(x,-99999998) as current_location_x,
        nvl(y,-99999998) as current_location_y,
        nvl(current_battery,0) as current_battery,
        pub_time,
        event_time
    from patrol_log
    lateral view json_tuple(description,'taskId', 'taskName', 'projectCode','projectName','avoidanceTime','x','y','battery') a
    as programme_id,programme_name,project_code_desc,project_name_desc,avoidance_time,x,y,current_battery
)
insert overwrite table cdmdwd.dwd_crio_tmd_patrol_obstacle_a_d
    select
        oid,
        event_id,
        robot_id,
        tenant_id,
        project_code,
        project_name,
        event_type,
        event_type_name,
        description,
        programme_id,
        programme_name,
        avoidance_time,
        current_location_x,
        current_location_y,
        current_battery,
        pub_time,
        event_time,
        'bj-prod-232' as k8s_env_name
    from obstacle_table;