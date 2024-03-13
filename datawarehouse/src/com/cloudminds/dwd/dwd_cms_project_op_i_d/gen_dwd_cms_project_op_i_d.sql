set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;
set mapreduce.job.queuename=root.users.liuhao;

insert overwrite table cdmdwd.dwd_cms_project_op_i_d partition(dt)
    select
    md5(concat(cast(id as string),op,updated)) as event_id,
    id as mid,
    name as project_name,
    nvl(upper(priority),'') as priority,
    case when length(import_date) = 10 then nvl(concat(from_unixtime( cast(import_date as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
         when length(import_date) = 19 then nvl(concat(import_date,'.000'),'')
         when length(import_date) >= 19 and length(import_date) < 23 then nvl(concat(REGEXP_REPLACE(REGEXP_REPLACE(import_date,'T',' '),'Z',''),'.000'),'')
         when length(import_date) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(import_date,'T|Z',' '),0,23),'')
         else nvl(concat(from_unixtime( cast(import_date/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(import_date,11,3)),'')
    end as import_date,
    case when length(start_date) = 10 then nvl(concat(from_unixtime( cast(start_date as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
         when length(start_date) = 19 then nvl(concat(start_date,'.000'),'')
         when length(start_date) >= 19 and length(start_date) < 23 then nvl(concat(REGEXP_REPLACE(REGEXP_REPLACE(start_date,'T',' '),'Z',''),'.000'),'')
         when length(start_date) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(start_date,'T|Z',' '),0,23),'')
         else nvl(concat(from_unixtime( cast(start_date/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(start_date,11,3)),'')
    end as start_date,
    case when length(end_date) = 10 then nvl(concat(from_unixtime( cast(end_date as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
         when length(end_date) = 19 then nvl(concat(end_date,'.000'),'')
         when length(end_date) >= 19 and length(end_date) < 23 then nvl(concat(REGEXP_REPLACE(REGEXP_REPLACE(end_date,'T',' '),'Z',''),'.000'),'')
         when length(end_date) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(end_date,'T|Z',' '),0,23),'')
         else nvl(concat(from_unixtime( cast(end_date/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(end_date,11,3)),'')
    end as end_date,
    nvl(category,'') as category,
    nvl(industry,'') as scene,
    nvl(trainer,'') as trainer_id,
    nvl(trainer_name,'') as trainer_name,
    nvl(operator,'') as operator_id,
    nvl(operator_name,'') as operator_name,
    nvl(content_operator,'') as content_operator_id,
    nvl(content_operator_name,'') as content_operator_name,
    case when trim(agents) = '[{}]' or trim(agents) = '[]' then ''
         else agents
    end as sv_agent_id,
    nvl(status,0) as status,
    case when status = 0 then '未开始'
         when status = 1 then '在运营'
         when status = 2 then '到期关闭'
         when status = 3 then '到期未关闭'
         when status = 4 then '已取消'
    end as status_name,
    nvl(op,'') as op,
    case when length(created) = 10 then nvl(concat(from_unixtime( cast(created as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
         when length(created) = 19 then nvl(concat(created,'.000'),'')
         when length(created) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(created,'T|Z',' '),0,23),'')
         else nvl(concat(from_unixtime( cast(created/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(created,11,3)),'')
    end as created_at,
    case when length(updated) = 10 then nvl(concat(from_unixtime( cast(updated as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
         when length(updated) = 19 then nvl(concat(updated,'.000'),'')
         when length(updated) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(updated,'T|Z',' '),0,23),'')
         else nvl(concat(from_unixtime( cast(updated/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(updated,11,3)),'')
    end as updated_at,
    case when length(updated) = 10 then nvl(concat(from_unixtime( cast(updated as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
         when length(updated) = 19 then nvl(concat(updated,'.000'),'')
         when length(updated) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(updated,'T|Z',' '),0,23),'')
         else nvl(concat(from_unixtime( cast(updated/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(updated,11,3)),'')
    end as event_time,
    'bj-prod-232' as k8s_env_name,
    case when length(updated) = 10 then to_date(nvl(concat(from_unixtime( cast(updated as int),'yyyy-MM-dd HH:mm:ss'),'.000'),''))
         when length(updated) = 19 then to_date(nvl(concat(updated,'.000'),''))
         when length(updated) >= 23 then to_date(nvl(SUBSTRING(REGEXP_REPLACE(updated,'T|Z',' '),0,23),''))
         else to_date(nvl(concat(from_unixtime( cast(updated/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(updated,11,3)),''))
    end as dt
    from cdmods.ods_db_c0022_crawl_ls_project_i_d where dt >= '${s_dt_var}' and dt <= '${e_dt_var}';