--  modify by dave.liang at 2021-09-17
set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;
set mapreduce.input.fileinputformat.input.dir.recursive=true;
set  hive.exec.max.dynamic.partitions.pernode=500;
set hive.merge.smallfiles.avgsize=128000000;
set hive.merge.size.per.task=128000000;
set mapreduce.job.queuename=root.users.liuhao;

insert overwrite table cdmdwd.dwd_cms_svo_fqa_i_d partition(dt) select
distinct
    nvl(id, '') as fqa_id ,
    nvl(cate_id, '') as fqa_cate_id,
    nvl(title,'') as title,
    case status when 1 then 0
                when 2 then 1
                else -99999998
        end as  is_work,
    case status when 1 then '失效'
                    when 2 then '生效'
                    else '未知'
    end as  is_work_name,
    case when length(add_time) = 10 then nvl(concat(from_unixtime( cast(add_time as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
         when length(add_time) = 19 then nvl(concat(add_time,'.000'),'')
         when length(add_time) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(add_time,'T',' '),0,23),'')
         else nvl(concat(from_unixtime( cast(add_time/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(add_time,11,3)),'')
        end as create_time,
    case when length(update_time) = 10 then nvl(concat(from_unixtime( cast(update_time as int),'yyyy-MM-dd HH:mm:ss'),'.000'),'')
         when length(update_time) = 19 then nvl(concat(update_time,'.000'),'')
         when length(update_time) >= 23 then nvl(SUBSTRING(REGEXP_REPLACE(update_time,'T',' '),0,23),'')
         else nvl(concat(from_unixtime( cast(update_time/1000 as int),'yyyy-MM-dd HH:mm:ss'),'.',substring(update_time,11,3)),'')
        end as event_time,
    nvl(push_action,-99999998) as push_action,
    case push_action when 1 then 'add'
                    when 2 then 'delete'
                    when 3 then 'update'
                    else 'unknown'
    end as  push_action_name,
    case need_push when 1 then 1
                when 2 then 0
                else -99999998
        end as  is_need_push,
    case is_del when 1 then 0
                when 2 then 1
                else -99999998
        end as  is_del,
    nvl(question, '') as question,
    nvl(answer,'') as answer,
    nvl(emoji,'') as emoji,
    nvl(last_seq_id,'') as last_seq_id,
    nvl(synstatus, -99999998) as is_syn_status,
    nvl(auditname, '') as audit_name,
    nvl(svmsg, '') as sv_msg,
    nvl(source,'') as source,
    cast(sys_cate_id as int ) as sys_cate_id,
    sv_cate_id,
    case bigdata_method when 'INSERT' then 1
                     when 'UPDATE' then 2
                     when 'DELETE' then 3
                     else -99999998
    end as op_db,
    'bj-prod-232' as k8s_env_name,
    dt
from
    cdmods.ods_cms_db_c0016_fqaitem_i_d
where
    dt >= '${s_dt_var}'
--     dt >= '2021-07-15'
and
    dt <= '${e_dt_var}'
--     dt <= '2021-07-15' limit  10
;


