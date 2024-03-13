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
      null_where_sql_str=" where ${array[i]} is null "
    else
      null_where_sql_str="$null_where_sql_str or ${array[i]} is null "
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
        add_count_query_result="select count(1) from cdmods.$table_name where dt ='$do_date'"
        total_count_query_result="select count(1) from cdmods.$table_name"
        table_nil_query_result="select count(1) from cdmods.$table_name $null_where_sql_str"
        table_duplicate_query_result="select count(1) from (select $null_column from cdmods.$table_name group by $null_column having count(1) >1 ) as tmp"
        echo "增量..."
        ;;
 "s")
        table_type="s"
        table_type_name="快照"
        add_count_query_result="SELECT t1.cnt - t2.cnt from ( SELECT 't' as t,count(1) as cnt from cdmods.$table_name where dt ='$do_date') t1 left join (SELECT 't' as t,count(1) as cnt from cdmods.$table_name where dt =date_sub('$do_date',1)) t2 on t1.t = t2.t"
        total_count_query_result="select count(1) from cdmods.$table_name where dt ='$do_date'"
        table_nil_query_result="select count(1) from cdmods.$table_name $null_where_sql_str and dt ='$do_date'"
        table_duplicate_query_result="select count(1) from (select $null_column from cdmods.$table_name where dt ='$do_date' group by $null_column having count(1) >1 ) as tmp"
        echo "快照 ..."
        ;;
 "a")
        table_type="a"
        table_type_name="全量"
        add_count_query_result="select count(1) from cdmods.$table_name"
        total_count_query_result="select count(1) from cdmods.$table_name"
        table_nil_query_result="select count(1) from cdmods.$table_name $null_where_sql_str"
        table_duplicate_query_result="select count(1) from (select $null_column from cdmods.$table_name group by $null_column having count(1) >1 ) as tmp"
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

# 根据table类型生成重复值查询sql


echo "----------------------检验增量记录数---------------------"
# 执行当日增量数据记录数量sql查询语句
# add_count_query_result=`hive -e "select count(1) from cdmods.$table_name where dt ='$do_date'"`
add_cnt_query_result=`beeline -u jdbc:hive2://bigdata1:10000/sv -n hive -p Cm@456123 -e "$add_count_query_result"`
# 取出当日增量数据记录数量
inc_cnt=$add_cnt_query_result
echo "+++++++++++++++++++++++++++$inc_cnt"
echo "----------------------检验全表记录数---------------------"
# 执行当日全表数据记录数量sql查询语句
# total_count_query_result=`hive -e "select count(1) from cdmods.$table_name"`
total_cnt_query_result=`beeline -u jdbc:hive2://bigdata1:10000/sv -n hive -p Cm@456123 -e "$total_count_query_result"`
# 取出当日全量数据记录数量
total_cnt=$total_cnt_query_result
echo "+++++++++++++++++++++++++++$total_cnt"
echo "----------------------检验空值记录数---------------------"
# 执行全表空值数据记录数量sql查询语句
# table_nil_query_result=`hive -e "select count(1) from cdmods.$table_name $null_where_sql_str"`
table_null_query_result=`beeline -u jdbc:hive2://bigdata1:10000/sv -n hive -p Cm@456123 -e "$table_nil_query_result"`
# 取出全表空值数据记录数量
nil_cnt=$table_null_query_result
echo "+++++++++++++++++++++++++++$nil_cnt"
echo "----------------------检验重复值记录数---------------------"
# 执行全表重复值的记录数量sql查询语句
# table_duplicate_query_result=`hive -e "select sum(tmp.duplicate_count) as duplicate_sum from (select count(1) as duplicate_count from cdmods.$table_name group by $null_column having count(1) >1 ) as tmp"`
table_dup_query_result=`beeline -u jdbc:hive2://bigdata1:10000/sv -n hive -p Cm@456123 -e "$table_duplicate_query_result"`
# 取出全表重复值数据记录数量
dup_cnt=$table_dup_query_result
echo "+++++++++++++++++++++++++++$dup_cnt"
echo "----------------------插入表---------------------"
#hive -e "insert into cdmdq.ods_data_check values('cdmods','$table_name','$table_type','$table_type_name',$total_cnt,$nil_cnt,$inc_cnt,$dup_cnt,'$do_date')"
# 写入到pg库
md=`md5sum <<< "'cdmods'$table_name$do_date"`
id=${md:0:32}
psql="insert into ods_data_check (id, database_name, table_name, table_type, table_type_name, total_cnt, nil_cnt, inc_cnt, dup_cnt, dt) values ('$id','cdmods','$table_name','$table_type','$table_type_name',$total_cnt,$nil_cnt,$inc_cnt,$dup_cnt,'$do_date') on conflict (id) do update set database_name=excluded.database_name, table_name=excluded.table_name, table_type=excluded.table_type, table_type_name=excluded.table_type_name, total_cnt=excluded.total_cnt, inc_cnt=excluded.inc_cnt, nil_cnt=excluded.nil_cnt, dup_cnt=excluded.dup_cnt, dt=excluded.dt"
echo $psql
export PGPASSWORD=cloud1688
psql -h 172.16.31.1 -p 32086 -U postgres -d cdmdq -c "$psql"
done <<<`hdfs dfs -cat /user/liuhao/hql/quality/ods_check/ods_check_conf.txt`