--  modify by dave.liang at 2021-09-22
set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;
set mapreduce.input.fileinputformat.input.dir.recursive=true;
set  hive.exec.max.dynamic.partitions.pernode=500;
set hive.merge.smallfiles.avgsize=128000000;
set hive.merge.size.per.task=128000000;
set mapreduce.job.queuename=root.users.liuhao;

insert overwrite table cdmdim.dim_svo_fqa_cate_sh_d partition(dt)
select
       nvl(dim_cate.fqa_cate_id, tmp1.id) as fqa_cate_id ,
       case when tmp1.id is not null  then nvl(tmp1.pid,'')
           else dim_cate.pid
       end as pid,
       case when tmp1.id is not null  then nvl(trim(tmp1.cate_name),'')
            else dim_cate.cate_name
       end as cate_name,
       cast( case when tmp1.id is not null  then tmp1.cates['level']
           else dim_cate.level end as int )as level,
       case when tmp1.id is not null  then ( case trim(lower(tmp1.status)) when 'yes' then 1
                                                              when 'no' then 0 end)
           else dim_cate.is_work
       end as is_work,
       case when tmp1.id is not null then (case trim(lower(tmp1.status)) when 'yes' then '生效'
                                                            when 'no' then '失效' end)
            else dim_cate.is_work_name
       end as is_work_name,
       case when tmp1.id is not null  then (case when length(tmp1.add_time) = 10 then nvl(concat(from_unixtime( cast(tmp1.add_time as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
                                                 when length(tmp1.add_time) = 19 then nvl(concat(tmp1.add_time,'.000'),'')
                                                 when length(tmp1.add_time) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(tmp1.add_time,'T',' '),0,23),'')
                                                 else nvl(concat(from_unixtime( cast(tmp1.add_time/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(tmp1.add_time,11,3)),'') end)
           else dim_cate.create_time
       end as create_time,
       case when tmp1.id is not null  then (case when length(tmp1.update_time) = 10 then nvl(concat(from_unixtime( cast(tmp1.update_time as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
                                                 when length(tmp1.update_time) = 19 then nvl(concat(tmp1.update_time,'.000'),'')
                                                 when length(tmp1.update_time) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(tmp1.update_time,'T',' '),0,23),'')
                                                 else nvl(concat(from_unixtime( cast(tmp1.update_time/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(tmp1.update_time,11,3)),'') end)
           else dim_cate.update_time
       end as update_time,
       case when tmp1.id is not null  then tmp1.push_action
           else dim_cate.push_action_name end as push_action_name,
       case when tmp1.id is not null  then (case tmp1.need_push when 'yes' then 1
                                                                when 'no' then 0 end)
           else dim_cate.is_need_push
       end as is_need_push,
       case when tmp1.id is not null  then ( case tmp1.is_del when 'yes' then 1
                                                              when 'no' then 0 end)
           else dim_cate.is_del
       end as is_del,
       case when tmp1.id is not null  then tmp1.last_seq_id
           else dim_cate.last_seq_id
       end as last_seq_id,
       case when tmp1.id is not null  then (case tmp1.synstatus when 'yes' then 1
                                                                when 'no' then 0 end)
           else dim_cate.is_syn_status end as is_syn_status,
       case when tmp1.id is not null  then  tmp1.auditname
           else dim_cate.audit_name
       end as audit_name,
       case when tmp1.id is not null  then tmp1.svmsg
           else dim_cate.sv_msg
       end as sv_msg,
       case when tmp1.id is not null  then tmp1.source
           else dim_cate.source
       end as source,
       case when tmp1.id is not null  then tmp1.k8s_env_name
           else dim_cate.k8s_env_name
       end as k8s_env_name,
       case when tmp1.id is not null  then (case when length(tmp1.add_time) = 10 then nvl(concat(from_unixtime( cast(tmp1.add_time as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
                                                 when length(tmp1.add_time) = 19 then nvl(concat(tmp1.add_time,'.000'),'')
                                                 when length(tmp1.add_time) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(tmp1.add_time,'T',' '),0,23),'')
                                                 else nvl(concat(from_unixtime( cast(tmp1.add_time/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(tmp1.add_time,11,3)),'') end)
           else dim_cate.start_time
       end as start_time,
       case when (tmp1.id is not null and tmp1.is_del='yes' )   then cast( to_utc_timestamp( (unix_timestamp(tmp1.update_time)*1000 -1), 'UTC') as string)
           else '9999-12-31 23:59:59.999' end as end_time,
       case when tmp1.id is not null  then replace(tmp1.cates['cate_name'],' ','')
           else dim_cate.cate_name_path end as cate_name_path,
       case when tmp1.id is not null  then tmp1.cates['id']
           else dim_cate.cate_id_path
       end as cate_id_path,
       '${e_dt_var}' as dt
from
     (select *
     from cdmdim.dim_svo_fqa_cate_sh_d
     where dt= date_add('${e_dt_var}',-1 ) and
           end_time = '9999-12-31 23:59:59.999'
         ) as dim_cate full join

    (select
            tmp.*,
            cdmudf.levelinfoudf('id','pid',"cdmods.ods_cms_db_c0016_fqacate_s_d where dt='${e_dt_var}'",array('id','cate_name'), id) as cates
    from cdmods.ods_cms_db_c0016_fqacate_s_d as tmp
    where update_time > '${e_dt_var}' and
          dt='${e_dt_var}'
    ) as tmp1  on dim_cate.fqa_cate_id = tmp1.id
union all
select
    dim_cate.fqa_cate_id,
    dim_cate.pid,
    dim_cate.cate_name,
    dim_cate.level,
    dim_cate.is_work,
    dim_cate.is_work_name,
    dim_cate.create_time,
    dim_cate.update_time,
    dim_cate.push_action_name,
    dim_cate.is_need_push,
    dim_cate.is_del,
    dim_cate.last_seq_id,
    dim_cate.is_syn_status,
    dim_cate.audit_name,
    dim_cate.sv_msg,
    dim_cate.source,
    dim_cate.k8s_env_name,
    dim_cate.start_time,
    cast( to_utc_timestamp( (unix_timestamp(tmp1.update_time)*1000 -1), 'UTC') as string) as end_time,
    dim_cate.cate_name_path,
    dim_cate.cate_id_path,
    '${e_dt_var}' as dt
from
    (select *
     from cdmdim.dim_svo_fqa_cate_sh_d
     where dt= date_add('${e_dt_var}',-1 ) and
             end_time = '9999-12-31 23:59:59.999'
    ) as dim_cate
        inner join (
            select *
            from  cdmods.ods_cms_db_c0016_fqacate_s_d
            where update_time > '${e_dt_var}' and
                        dt='${e_dt_var}' and
                        is_del = 'no')  as tmp1
        on dim_cate.fqa_cate_id = tmp1.id



