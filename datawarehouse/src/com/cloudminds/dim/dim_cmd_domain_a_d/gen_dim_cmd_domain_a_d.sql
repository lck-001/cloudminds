SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.merge.mapfiles = true;
SET hive.merge.mapredfiles = true;
SET hive.exec.max.dynamic.partitions = 60000;
set mapreduce.job.queuename=root.users.liuhao;

INSERT OVERWRITE TABLE cdmdim.dim_cmd_domain_a_d
SELECT 
    nvl(id, '') as domain_id,
    regexp_replace(nvl(trim(domainname),''), '\r|\n|\t', '') as domain_name,
    regexp_replace(nvl(trim(domaininfo),''), '\r|\n|\t', '') as domain_info,
    nvl(type, '') as domain_type,
    nvl(status, '') as status,
    nvl(`url`, '') as url,
    nvl(callservice, '') as callservice,
    case when release = 1 or release = '1' then 1
         else 0
    end as is_release,
    case when closed_recg = 0 or closed_recg = '0' then 0
         else 1
    end as is_closed_recg,
    nvl(closed_hit, '') as closed_hit,
    nvl(keyword, -99999998) as keyword,
    nvl(domain_switch_hit, '') as domain_switch_hit,
    case when nvl(k8s_env_name,'') != '' then k8s_env_name
         else 'bj-prod-232'
    end as k8s_env_name,
    nvl(from_unixtime(createtime, 'yyyy-MM-dd HH:mm:ss.SSS'), '') as create_time,
    nvl(from_unixtime(createtime, 'yyyy-MM-dd HH:mm:ss.SSS'), '') as update_time
FROM cdmods.ods_sv_db_c0003_domain_s_d where dt = '${e_dt_var}';
