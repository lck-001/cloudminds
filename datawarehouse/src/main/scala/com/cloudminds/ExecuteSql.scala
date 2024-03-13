package com.cloudminds


import org.apache.spark.sql
import org.apache.spark.sql.DataFrame
import org.slf4j.{Logger, LoggerFactory}

import java.util
import scala.collection.mutable.ListBuffer

object ExecuteSql extends SparkSqlConf("execute sql") {
  private val log = LoggerFactory.getLogger("Test_Logger")
  val err_lst = new ListBuffer[String]()
  val msg_lst = new ListBuffer[String]()
  def main(args: Array[String]): Unit = {
    val conf_f_dir = args(0)
    val s_dt_var = args(1)
    val e_dt_var = args(2)
    execute(conf_f_dir,s_dt_var,e_dt_var)
    spark.close()
  }
  def execute(conf_f_dir: String, s_dt_var: String, e_dt_var: String): Unit ={
    val conf_name = conf_f_dir.split("/").last
    if (HdfsUtils.isExists(file_sys, conf_f_dir)){
      //读配置文件
      val conf_f_arr = sc.textFile(conf_f_dir, 1)
        .filter(line => line.trim != "" && (!line.contains("---------")) && (!line.trim.startsWith("--")))
        .collect()
      for (str <- conf_f_arr) {
        val sql_file = str.split("\\|").head.trim
        val sql_file_name = sql_file.split("/").last
//        val pattern=""".*gen_(.*).sql""".r
//        var table_name = ""
//        sql_file_name match {
//          case pattern(str) => table_name = str
//          case _ => println("not match")
//        }
//        val tableInfo = table_name.split("_")
        val tableInfo = sql_file_name.split("_")
        val table_name = sql_file_name
//        var baseUrl = "/tmp/hql/script/"
        var baseUrl = "/data/cdm_script/script/dq_script/script/"
        baseUrl=tableInfo(0) match {
          case "ods" => baseUrl+"ods/"
          case "dwd" => baseUrl+"dwd/"
          case "dim" => baseUrl+"dim/"
          case "dwm" => baseUrl+"dwm/"
          case _ => throw new Exception("no config find")
        }
        baseUrl=tableInfo(tableInfo.size-2) match {
          case "i" => baseUrl+"i.sql"
          case "a" => baseUrl+"a.sql"
          case "s" => baseUrl+"s.sql"
          case "sh" => baseUrl+"sh.sql"
          case _ => throw new Exception("no sql file find")
        }

        val sql_arr = sqlConcat(baseUrl, str, e_dt_var)

        run_sql(sql_arr, table_name, sql_file)
//        if (HdfsUtils.isExists(file_sys,sql_file)){
//
//        }else {
//          //sql脚本不存在写入相应信息
//          err_lst += s"Error: sql script = $conf_name -- $sql_file, is not exists!!!"
//        }
      }
    }
  }

  def sqlConcat(baseUrl:String, configLine:String, e_dt_var:String):Array[String] = {
    val tableInfo = configLine.split("\\|")
    val tableName = tableInfo(0).trim
    val columns = tableInfo(1).trim
    val dimColumnArr = columns.split("###")
    val tableType = tableName.split("_")(0)
    if ("dwm".equalsIgnoreCase(tableType)){
      val sqlRdd = sc.textFile(baseUrl, 1)
        .filter(line => (!line.contains("set hive.")) && line.trim != "" && (!line.trim.startsWith("--")))
      val sqlTextArr = sqlRdd.collect()
      val sqlStr = sqlTextArr.mkString("\n")
      val sqlArr = sqlStr.split(";")
      val tmpSql = new ListBuffer[String]()
      // 必须传当前维度的id信息
      for (dimColumn <- dimColumnArr){
        val columns = dimColumn.split(",")
        var i = 0
        var null_where_sql_str = ""
        for (column <- columns if columns.nonEmpty){
          if (i == 0){
            i+=1
          }else if(i == 1){
            null_where_sql_str = s" ( $column is null or trim(cast($column as string)) = '' ) "
          }else{
            null_where_sql_str = s"$null_where_sql_str or ( $column is null or trim(cast($column as string)) = '' )"
          }
        }
        if (null_where_sql_str.nonEmpty){
          val dim_id = columns(0)
          null_where_sql_str = s"where ( $null_where_sql_str ) and $dim_id is not null and trim(cast($dim_id as string)) != '' "
        }
//        val withSql = sqlArr(0).split(",")(0)
        val pattern=""".+\sas\(([^)]+?)\)""".r
//        val headSql = pattern.findAllMatchIn(withSql).map(_.group(1)).toList.head
        val headSql = pattern.findFirstMatchIn(sqlArr(0)).map(_.group(1)).getOrElse("")
        val sql = headSql.replace("$table_name", tableName)
          .replace("${e_dt_var}", e_dt_var)
          .replace("$dim", columns(0).split("__")(0))
          .replace("$id",columns(0))
          .replace("$null_where_sql_str", null_where_sql_str)
        tmpSql.append(sql)
      }
      val submitSql = tmpSql.mkString(" \n union all \n ")
      sqlArr(0) = sqlArr(0).replaceAll("""(?<=as\()([^)]+?)(?=\))""",submitSql)
      val arr = sqlArr.map(_.replace("$table_name", tableName).replace("${e_dt_var}", e_dt_var))
      val sqls = arr.mkString(";")
      val sql_arr = sqls.split("\n")
      sql_arr
    }else{
      var null_where_sql_str = ""
      val null_columns_arr = columns.split(",")
      var i = 0
      for (col <- null_columns_arr){
        if (i == 0){
          null_where_sql_str = s"where ( $col is null or trim(cast($col as string)) = '' ) "
          i=i+1
        }else{
          null_where_sql_str = s"$null_where_sql_str or ( $col is null or trim(cast($col as string)) = '' )"
        }
      }
      val sql_arr = sc.textFile(baseUrl, 1)
        .filter(line => (!line.contains("set hive.")) && line.trim != "" && (!line.trim.startsWith("--")))
        .map(_.replace("$table_name", tableName)
          .replace("${e_dt_var}",e_dt_var)
          .replace("$null_columns",columns)
          .replace("$null_where_sql_str",null_where_sql_str))
        .collect()
      sql_arr
    }
  }

  /*def sqlConcat(baseUrl:String, configLine:String, e_dt_var:String):Array[String] = {
    val tableInfo = configLine.split("\\|")
    val tableName = tableInfo(0).trim
    val columns = tableInfo(1).trim
    val columnArray = columns.split(",")
    val tableType = tableName.split("_")(0)
    if ("dwm".equalsIgnoreCase(tableType)){
      val columnMap = new util.HashMap[String, util.ArrayList[String]]()
      // 把查询相同属性的 都放在一个list中方便循环拼接
      var columnList:util.ArrayList[String] = null
      for (column <- columnArray){
        val table = column.split("__")
        if (columnMap.get(table(0)) == null) {
          columnList = new util.ArrayList[String]()
        }else{
          columnList = columnMap.get(table(0))
        }
        if (table(1) != null && table(1).nonEmpty ){
          columnList.add(column)
          columnMap.put(table(0),columnList)
        }
      }
      //循环拼接sql
      import collection.JavaConverters._
      val scalaMap: scala.collection.mutable.Map[String, util.ArrayList[String]] = columnMap.asScala
      val tmpSql = new ListBuffer[String]()
      val sqlRdd = sc.textFile(baseUrl, 1)
        .filter(line => (!line.contains("set hive.")) && line.trim != "" && (!line.trim.startsWith("--")))
      val sqlTextArr = sqlRdd.collect()
      val sqlStr = sqlTextArr.mkString("\n")
      val sqlArr = sqlStr.split(";")
      for ((k,v) <- scalaMap){
        var i = 0
        var null_where_sql_str = ""
        for (col <- v.asScala if !v.isEmpty){
          val cols = col.split("__")
          if (!cols.last.equalsIgnoreCase(cols(0)+"_id")){
            if (i == 0){
              null_where_sql_str = s" ( $col is null or trim(cast($col as string)) = '' ) "
              i=i+1
            }else{
              null_where_sql_str = s"$null_where_sql_str or ( $col is null or trim(cast($col as string)) = '' )"
            }
          }
        }
        if (null_where_sql_str.nonEmpty){
          null_where_sql_str = s"where ( $null_where_sql_str ) and $k+__+$k+_id is not null and trim(cast($k+__+$k+_id as string)) != '' "
        }
        val sql = sqlArr(0).replace("$table_name", tableName)
          .replace("${e_dt_var}", e_dt_var)
          .replace("$dim", k)
          .replace("$null_where_sql_str", null_where_sql_str)
        tmpSql.append(sql)
      }
      val submitSql = tmpSql.mkString(" \n union all \n ")
      sqlArr(0) = submitSql
      val arr = sqlArr.map(_.replace("$table_name", tableName).replace("${e_dt_var}", e_dt_var))
      val sqls = arr.mkString(";")
      val sql_arr = sqls.split("\n")
      sqlArr

    }else{
      var null_where_sql_str = ""
      val null_columns_arr = columns.split(",")
      var i = 0
      for (col <- null_columns_arr){
        if (i == 0){
          null_where_sql_str = s"where ( $col is null or trim(cast($col as string)) = '' ) "
          i=i+1
        }else{
          null_where_sql_str = s"$null_where_sql_str or ( $col is null or trim(cast($col as string)) = '' )"
        }
      }
      val sql_arr = sc.textFile(baseUrl, 1)
        .filter(line => (!line.contains("set hive.")) && line.trim != "" && (!line.trim.startsWith("--")))
        .map(_.replace("$table_name", tableName)
          .replace("${e_dt_var}",e_dt_var)
          .replace("$null_columns",columns)
          .replace("$null_where_sql_str",null_where_sql_str))
        .collect()
      sql_arr
    }
  }*/


  def run_sql(sql_arr: Array[String], conf_name: String, sql_file: String): Unit ={
    val lb = new ListBuffer[String]()
    for (line <- sql_arr){
      if (line.trim != ""){
        val line_split = line.split("--")
        if (line_split.head.trim.last == ';'){
          lb.+=(line_split.head.trim.replaceAll(";$", "")) //替换最后一个分号再拼装sql
          val sql = lb.mkString("\n")
          val flag = line_split.last.trim.split(":").head
          try {
            flag match {
              case "update_into_pgsql" =>
                log.info(sql)
                val res = spark.sql(sql)
                update_into_pgsql(line_split, res, sql_file)
              case "drop_temp_view" =>
                spark.catalog.dropTempView(line_split.last.split(":").last.trim)
              case _ =>
                log.info(sql)
                spark.sql(sql)
            }
          } catch {
            case e: Exception =>
              val error_table = s"$conf_name: ${sql_file.split("/").last.split("\\.").head}"
              err_lst += error_table //抛异常的表名写入error list
              msg_lst += s"\n----------------------------------$error_table----------------------------------------\n"
              msg_lst += e.getMessage //异常信息写入message list
              log.info(e.getMessage)
          }
          lb.clear()
        }else {
            lb.+=(line) //sql语句组装
          }
      }
    }
  }

  def update_into_pgsql(line_spilt: Array[String], data: sql.DataFrame, sql_file:String): Unit ={
    //user password ip port database table
    val db = line_spilt.last
    val dbInfo = db.split(":")

    val username = dbInfo(1).trim
    val password = dbInfo(2).trim
    val ip = dbInfo(3).trim
    val port = dbInfo(4).trim
    val database = dbInfo(5).trim
    val table_name = dbInfo(6).trim

    val special = if (dbInfo.length >= 9) List[Int](dbInfo(8).trim.toInt) else List[Int]()

    val rdbmsUtil = new RdbmsUtil(username, password, ip, port, database, table_name)

    rdbmsUtil.insertOrUpdateData(rdbmsUtil, data,database, table_name,s=special, sql_file)
    rdbmsUtil.closeConnection()

  }
}
