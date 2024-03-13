#!/bin/bash
kinit -kt /opt/soft/spark-2.4.0-bin-hadoop2.7/conf/hive.keytab hive/l-172016031107-bigdata.cloudminds.com@CLOUDMINDS.COM
while read myline
do
lines=(${myline//|/ })
# 增量数据所在的日期分区
do_date=$1
# 校验数据的表名
table_name=${lines[0]}
#需要校验空值的列名，以逗号','隔开
null_column=${lines[1]}
#初始化sql查询语句
null_where_sql_str=''
#将空值检验字符串切成列名数组
array=(${null_column//,/ })
# 遍历数组,拼接空值查询条件
for (( i=0; i<${#array[@]}; i++ )); do
    if [ $i -eq 0 ];then
      null_where_sql_str=" where ${array[i]} is null or trim(${array[i]}) = '' "
    else
      null_where_sql_str="$null_where_sql_str or ${array[i]} is null or trim(${array[i]}) = '' "
    fi
done
# 判断table是什么类型的表
table_arr=(${table_name//_/ })
table_type=''
table_type_name=''
# 根据表类型,拼接不同的sql参数
add_count_query_result=''
total_count_query_result=''
table_nil_query_result=''
table_duplicate_query_result=''
case ${table_arr[-2]} in
 "i")
        table_type="i"
        table_type_name="增量"
        all_query_result="select
                          t.k8s_env_name,
                          nvl(t1.add_count,0) as add_count,
                          nvl(t2.total_cnt,0) as total_cnt,
                          nvl(t3.nil_cnt,0) as nil_cnt,
                          nvl(t4.dup_cnt,0) as dup_cnt
                          from
                          (select k8s_env_name from cdmdwd.$table_name group by k8s_env_name) t
                          left join (
                          select k8s_env_name,count(1) as add_count from cdmdwd.$table_name where dt ='$do_date' group by k8s_env_name
                          ) t1 on t.k8s_env_name = t1.k8s_env_name
                          left join (
                          select k8s_env_name,count(1) as total_cnt from cdmdwd.$table_name group by k8s_env_name
                          ) t2 on t.k8s_env_name = t2.k8s_env_name
                          left join (
                          select k8s_env_name,count(1) as nil_cnt from cdmdwd.$table_name $null_where_sql_str group by k8s_env_name
                          ) t3 on t.k8s_env_name = t3.k8s_env_name
                          left join (
                          select k8s_env_name,count(1) as dup_cnt from (select $null_column,k8s_env_name from cdmdwd.$table_name group by $null_column, k8s_env_name having count(1) >1 ) as tmp group by k8s_env_name
                          ) t4 on t.k8s_env_name = t4.k8s_env_name"
        echo "增量..."
        ;;
 "s")
        table_type="s"
        table_type_name="快照"
        all_query_result="select
                          t.k8s_env_name,
                          nvl(t1.add_count,0) as add_count,
                          nvl(t2.total_cnt,0) as total_cnt,
                          nvl(t3.nil_cnt,0) as nil_cnt,
                          nvl(t4.dup_cnt,0) as dup_cnt
                          from
                          (select k8s_env_name from cdmdwd.$table_name group by k8s_env_name) t
                          left join (
                          SELECT t1.k8s_env_name,t1.cnt - t2.cnt from ( SELECT k8s_env_name,count(1) as cnt from cdmdwd.$table_name where dt ='$do_date' group by k8s_env_name) t1 left join (SELECT k8s_env_name,count(1) as cnt from cdmdwd.$table_name where dt =date_sub('$do_date',1) group by k8s_env_name ) t2 on t1.k8s_env_name = t2.k8s_env_name
                          ) t1 on t.k8s_env_name = t1.k8s_env_name
                          left join (
                          select k8s_env_name,count(1) as total_cnt from cdmdwd.$table_name where dt ='$do_date' group by k8s_env_name
                          ) t2 on t.k8s_env_name = t2.k8s_env_name
                          left join (
                          select k8s_env_name,count(1) as nil_cnt from cdmdwd.$table_name $null_where_sql_str and dt ='$do_date' group by k8s_env_name
                          ) t3 on t.k8s_env_name = t3.k8s_env_name
                          left join (
                          select k8s_env_name,count(1) as dup_cnt from (select $null_column,k8s_env_name from cdmdwd.$table_name where dt ='$do_date' group by $null_column, k8s_env_name having count(1) >1 ) as tmp group by k8s_env_name
                          ) t4 on t.k8s_env_name = t4.k8s_env_name"
        echo "快照 ..."
        ;;
 "a")
        table_type="a"
        table_type_name="全量"
        all_query_result="select
                          t.k8s_env_name,
                          nvl(t1.add_count,0) as add_count,
                          nvl(t2.total_cnt,0) as total_cnt,
                          nvl(t3.nil_cnt,0) as nil_cnt,
                          nvl(t4.dup_cnt,0) as dup_cnt
                          from
                          (select k8s_env_name from cdmdwd.$table_name group by k8s_env_name) t
                          left join (
                          select k8s_env_name,count(1) as add_count from cdmdwd.$table_name where dt ='$do_date' group by k8s_env_name
                          ) t1 on t.k8s_env_name = t1.k8s_env_name
                          left join (
                          select k8s_env_name,count(1) as total_cnt from cdmdwd.$table_name group by k8s_env_name
                          ) t2 on t.k8s_env_name = t2.k8s_env_name
                          left join (
                          select k8s_env_name,count(1) as nil_cnt from cdmdwd.$table_name $null_where_sql_str group by k8s_env_name
                          ) t3 on t.k8s_env_name = t3.k8s_env_name
                          left join (
                          select k8s_env_name,count(1) as dup_cnt from (select $null_column,k8s_env_name from cdmdwd.$table_name group by $null_column, k8s_env_name having count(1) >1 ) as tmp group by k8s_env_name
                          ) t4 on t.k8s_env_name = t4.k8s_env_name"
        echo "全量..."
        ;;
 "sh")
        table_type="sh"
        table_type_name="拉链"
        echo "拉链..."
        ;;
 *)
        echo ""
        ;;
esac

# 执行查询sql,获取查询结果
query_result=`hive -e "$all_query_result"`
# 拼接结果插入语句
hql="insert into cdmdq.dwd_data_check values "
pql="insert into dwd_data_check (id, database_name, table_name, table_type, table_type_name, k8s_env_name, inc_cnt, total_cnt, nil_cnt, dup_cnt, dt) values "
# 遍历查询结果,将结果封装到hive sql
while read -r line
do
arr=(${line// / })
echo ${arr[0]}
echo ${arr[1]}
echo ${arr[2]}
echo ${arr[3]}
echo ${arr[4]}
hql="$hql ('cdmdwd','$table_name','$table_type','$table_type_name','${arr[0]}',${arr[1]},${arr[2]},${arr[3]},${arr[4]},'$do_date'),"
md=`md5sum <<< "'cdmdwd'$table_name$do_date${arr[0]}"`
id=${md:0:32}
pql="$pql ('$id','cdmdwd','$table_name','$table_type','$table_type_name','${arr[0]}',${arr[1]},${arr[2]},${arr[3]},${arr[4]},'$do_date'),"
done <<< "$query_result"
# 拼接后的sql 判断最后是不是逗号符号,如果是清除
if [ ${hql: -1} == "," ];then
hql=${hql%,*}
else
$hql
fi
# 插入到hive表中
hive -e "$hql"
# 拼接后的sql 判断最后是不是逗号符号,如果是清除
if [ ${pql: -1} == "," ];then
pql=${pql%,*}
else
$pql
fi
echo $pql
psql=$pql" on conflict (id) do update set database_name=excluded.database_name, table_name=excluded.table_name, table_type=excluded.table_type, table_type_name=excluded.table_type_name, k8s_env_name=excluded.k8s_env_name, inc_cnt=excluded.inc_cnt, total_cnt=excluded.total_cnt, nil_cnt=excluded.nil_cnt, dup_cnt=excluded.dup_cnt, dt=excluded.dt"
echo $psql
export PGPASSWORD=cloud1688
psql -h 172.16.31.1 -p 32086 -U postgres -d cdmdq -c "$psql"
done <<<`hdfs dfs -cat /user/liuhao/hql/quality/dwd_check/dwd_check_conf.txt`