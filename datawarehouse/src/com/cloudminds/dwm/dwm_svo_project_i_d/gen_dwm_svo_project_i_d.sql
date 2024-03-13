set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;
set mapreduce.job.queuename=root.users.liuhao;
insert overwrite table cdmdwm.dwm_svo_project_i_d PARTITION(dt)
select 
    mid,
    project_name,
    priority,
    category,
    scene,
    trainer_id,
    trainer_name,
    operator_id,
    operator_name,
    content_operator_id,
    content_operator_name,
    status,
    status_name,
    k8s_env_name,
    import_date,
    event_time,
    sv_agent_id,
    '${e_dt_var}' as dt
 from (
    select 
      mid,
      project_name,
      priority,
      category,
      scene,
      trainer_id,
      trainer_name,
      operator_id,
      operator_name,
      content_operator_id,
      content_operator_name,
      status,
      status_name,
      k8s_env_name,
      import_date,
      event_time,
      op,
      sv_agent_id,
      row_number() over (
        PARTITION by mid order by updated_at desc
      ) as rnk
    from cdmdwd.dwd_cms_project_op_i_d
    where
      to_date(import_date) <= '${e_dt_var}'
    and 
      to_date(event_time) <= '${e_dt_var}'
)t
where t.rnk = 1 and t.op != 'd';
