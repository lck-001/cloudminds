set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;
set mapreduce.job.queuename=root.users.liuhao;

--232环境的快照数据
with historyData232 as (
     select
        robot_code as robot_id,
        tenant_code as tenant_id,
        user_code as robot_account_id,
        rcu_code as rcu_id,
        token as token,
        status as status,
        case status when -1 then '删除'
                    when 0 then '注册未激活'
                    when 1 then '激活可用'
                    when 2 then '通知VPN注册'
                    else '未知' end as status_name,
        1 as db_op,
        CONCAT(update_time,".000") as event_time,
        update_time as update_time,
        create_time as create_time,
        'bj-prod-232' as k8s_env_name,
     from cdmods.ods_roc_db_c0002_t_user_rcu_robot_s_d where dt='${e_dt_var}'),

--232环境的binlog数据
    data232binlog as (
     select
        robot_code as robot_id,
        tenant_code as tenant_id,
        user_code as robot_account_id,
        rcu_code as rcu_id,
        token as token,
        status as status,
        case status when -1 then '删除'
                    when 0 then '注册未激活'
                    when 1 then '激活可用'
                    when 2 then '通知VPN注册'
                    else '未知' end as status_name,
        case bigdata_method when 'INSERT' then 1
                                    when 'UPDATE' then 2
                                    else 3 end as db_op,
        concat(from_unixtime(cast(event_time/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.000') as event_time,
        update_time as update_time,
        create_time as create_time,
        'bj-prod-232' as k8s_env_name
     from cdmods.ods_roc_db_c0002_t_user_rcu_robot_i_d where dt>='${e_dt_var}'),

--251环境的快照数据
     historyData251 as (
     select
        robot_code as robot_id,
        tenant_code as tenant_id,
        user_code as robot_account_id,
        rcu_code as rcu_id,
        token as token,
        status as status,
        case status when -1 then '删除'
                    when 0 then '注册未激活'
                    when 1 then '激活可用'
                    when 2 then '通知VPN注册'
                    else '未知' end as status_name,
        1 as db_op,
        CONCAT(update_time,".000") as event_time,
        update_time as update_time,
        create_time as create_time,
        'bj-prod-251' as k8s_env_name,
        to_date(update_time) as dt
     from cdmods.ods_roc_db_c0008_t_user_rcu_robot_s_d where dt='${e_dt_var}'),

--251环境的binlog数据
     data251binlog as (
     select
        robot_code as robot_id,
        tenant_code as tenant_id,
        user_code as robot_account_id,
        rcu_code as rcu_id,
        token as token,
        status as status,
        case status when -1 then '删除'
                    when 0 then '注册未激活'
                    when 1 then '激活可用'
                    when 2 then '通知VPN注册'
                    else '未知' end as status_name,
        case bigdata_method when 'INSERT' then 1
                            when 'UPDATE' then 2
                            else 3 end as db_op,
        concat(from_unixtime(cast(event_time/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.000') as event_time,
        update_time as update_time,
        create_time as create_time,
        'bj-prod-251' as k8s_env_name
     from cdmods.ods_roc_db_c0008_t_user_rcu_robot_i_d where dt>='${e_dt_var}'),

--所有数据合并并去重
     allData as (
     select
        *,
        row_number() over(partition by
                               robot_id,
                               tenant_id,
                               robot_account_id,
                               rcu_id,
                               token,
                               status,
                               update_time,
                               create_time,
                               k8s_env_name ORDER BY db_op ASC) as rnk
     from (select * from historyData232 union all select * from data232binlog union all select * from historyData251 union all select * from data251binlog) a)

insert overwrite table cdmdwd.dwd_roc_pmd_user_rcu_robot_a_d
    select
       robot_id,
       tenant_id,
       robot_account_id,
       rcu_id,
       token,
       status,
       status_name,
       db_op,
       event_time,
       k8s_env_name
    from allData where rnk=1;