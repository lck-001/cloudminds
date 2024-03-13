set hive.exec.dynamic.partition.mode = nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.exec.max.dynamic.partitions = 60000;
set mapreduce.job.queuename=root.prod;

with tmp as (
  select
    svsa0001001002,
    case when svsa0001001002<=1000 then '(0,1]'
         when svsa0001001002<=2000 then '(1,2]'
         when svsa0001001002<=3000 then '(2,3]'
         when svsa0001001002<=4000 then '(3,4]'
         when svsa0001001002<=5000 then '(4,5]'
         when svsa0001001002<=8000 then '(6,8]'
         when svsa0001001002<=10000 then '(8,10]'
         when svsa0001001002<=12000 then '(10,12]'
         when svsa0001001002<=14000 then '(12,14]'
         when svsa0001001002<=16000 then '(14,16]'
         when svsa0001001002<=18000 then '(16,18]'
         when svsa0001001002<=20000 then '(18,20]'
         else '(20,++]'
         end as duration_scop,
    svsa1001001008,
    svsa0002001001,
    svsa0001002001,
    svsa0001002002,
    svsa0003002001
  from cdmods.ods_sv_event_audio_a_manual
)
insert overwrite table cdmdws.dws_cmd_sv_statistic_a_manual
    select
      count(*) as cnt,
      sum(svsa0001001002) as duration,
      sum(svsa1001001008) as file_size,
      duration_scop,
      svsa0002001001,
      svsa0001002001,
      svsa0001002002,
      svsa0003002001
    from tmp group by duration_scop,svsa0002001001,svsa0001002001,svsa0001002002,svsa0003002001;