set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;
set mapreduce.job.queuename=root.users.liuhao;

--232环境的binlog数据
with data232 as (
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
        'bj-prod-232' as k8s_env_name
     from cdmods.ods_roc_db_c0002_t_user_rcu_robot_i_d where dt='${e_dt_var}'),

--251环境的binlog数据
     data251 as (
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
        'bj-prod-251' as k8s_env_name
     from cdmods.ods_roc_db_c0008_t_user_rcu_robot_i_d where dt='${e_dt_var}'),
--两者数据和历史数据一起去重
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
                               bigdata_method,
                               db_op,
                               event_time,
                               k8s_env_name ORDER BY event_time ASC) as rnk
     from (select * from data232 union all select * from data251 union all select * from cdmdwd.dwd_roc_pmd_user_rcu_robot_a_d) a)

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