set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;
set mapreduce.job.queuename=root.prod;

with union_data as(
   select
      *,
      row_number() over (partition by id ORDER BY update_time desc) as rnk
   from (select
            id,
            pid,
            cate_name,
            level,
            sort,
            status,
            add_time,
            update_time,
            auditname,
            agent_id,
            source,
            uuid,
            memo,
            pub_id,
            k8s_env_name,
            'c' as bigdata_method
         from cdmtmp.tmp_ods_cms_qacate_agent_s_d_tanqiong_20780302
         where dt=date_sub('${e_dt_var}',1)
         union all
         select
            id,
            pid,
            cate_name,
            level,
            sort,
            status,
            nvl(concat(from_unixtime(cast(add_time/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(add_time,11)),'') as add_time,
            nvl(concat(from_unixtime(cast(update_time/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(update_time,11)),'') as update_time,
            auditname,
            agent_id,
            source,
            uuid,
            memo,
            pub_id,
            k8s_env_name,
            bigdata_method
         from cdmods.ods_cms_db_c0016_qacate_agent_i_d
         where dt='${e_dt_var}') t)
insert overwrite table cdmtmp.tmp_ods_cms_qacate_agent_s_d_tanqiong_20780302 partition(dt)
    select
      id,
      pid,
      cate_name,
      level,
      sort,
      status,
      add_time,
      update_time,
      auditname,
      agent_id,
      source,
      uuid,
      memo,
      pub_id,
      k8s_env_name,
      '${e_dt_var}' as dt
    from union_data where rnk=1 and bigdata_method!='d'