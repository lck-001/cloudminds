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

insert overwrite table cdmdim.dim_svo_user_qa_cate_sh_d partition(dt)
select
    nvl(id, '') as user_qa_cate_id ,
    nvl(pid,'') as pid,
    nvl(trim(cate_name),'') as cate_name,
    cates['level'] as level,
    case status when 'yes' then 1
                when 'no' then 0 end as is_work,
    case when trim(lower(status)) = 'yes' then '生效'
         when trim(lower(status)) = 'no' then '失效'
    end as is_work_name,
    case when length(add_time) = 10 then nvl(concat(from_unixtime( cast(add_time as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
         when length(add_time) = 19 then nvl(concat(add_time,'.000'),'')
         when length(add_time) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(add_time,'T',' '),0,23),'')
         else nvl(concat(from_unixtime( cast(add_time/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(add_time,11,3)),'')
    end as create_time,
    case when length(update_time) = 10 then nvl(concat(from_unixtime( cast(update_time as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
         when length(update_time) = 19 then nvl(concat(update_time,'.000'),'')
         when length(update_time) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(update_time,'T',' '),0,23),'')
         else nvl(concat(from_unixtime( cast(update_time/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(update_time,11,3)),'')
    end as update_time,
    auditname as audit_name,
    agent_id,
    source,
    uuid as sv_msg,
    k8s_env_name,
    case when length(add_time) = 10 then nvl(concat(from_unixtime( cast(add_time as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
         when length(add_time) = 19 then nvl(concat(add_time,'.000'),'')
         when length(add_time) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(add_time,'T',' '),0,23),'')
         else nvl(concat(from_unixtime( cast(add_time/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(add_time,11,3)),'')
    end as start_time,
    '9999-12-31 23:59:59.999' as end_time,
    replace(cates['cate_name'],' ','') as cate_name_path,
    cates['id'] as cate_id_path,
    dt
from

    (select
         tmp.*,
         cdmudf.levelinfoudf('id','pid',"cdmtmp.tmp_ods_cms_qacate_agent_s_d_tanqiong_20780302 where dt='${e_dt_var}'",array('id','cate_name'), id) as cates
     from cdmtmp.tmp_ods_cms_qacate_agent_s_d_tanqiong_20780302 as tmp
     where dt='${e_dt_var}'
    ) as tmp1  ;