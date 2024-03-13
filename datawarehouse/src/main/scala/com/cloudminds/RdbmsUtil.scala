package com.cloudminds
import org.apache.spark.sql.{DataFrame, Row}

import java.sql.{Connection, DriverManager, ResultSet}
import scala.collection.mutable.ListBuffer

class RdbmsUtil(val username:String,val password:String,val ip:String,val port:String,val database:String,val table:String) {
//  private val url = s"jdbc:postgresql://$ip:$port/$database?useSSL=false"
  private val url = s"jdbc:postgresql://$ip:$port/$database"
  private var connection:Connection = _

  /**
   * 获取链接
   * @return conn
   */
  def getConnection:Connection={
    if (connection == null){
      try(
        Class.forName("org.postgresql.Driver").newInstance()
      )catch {
        case e:Exception=>println(e.printStackTrace())
      }
      connection = DriverManager.getConnection(url, username, password)
    }
    connection
  }
  /**
   * 关闭连接
   */
  def closeConnection(): Unit = {
    try {
      if (connection != null || !connection.isClosed) {
        connection.close()
      }
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    }
  }

  /**
   * 执行sql
   */
//  def executeSQL(connection: Connection,sql:String,data:Array[Array[String]],s: Seq[Int] = Seq()): Unit ={
//    try {
//      val ps = connection.prepareStatement(sql)
//      var i = 0L
//      for (line <- data) {
//        for (index <- 0 until (line.length - s.length)) {
//          if (line(index).isEmpty || line(index).toLowerCase() == "null")
//            ps.setObject(index + 1, null)
//          else
//            ps.setObject(index + 1, line(index))
//        }
//        s.foreach(j => {
//          if (line(j).isEmpty || line(j).toLowerCase() == "null") {
//            ps.setObject(j + 1, null)
//          } else {
//            ps.setBytes(j + 1, line(j).getBytes)
//          }
//        }) //getBytes可支持Emoji表情，但表和库及字段字符类型需要utf8mb4
//        ps.addBatch()
//        i = i + 1
//        if (i % 3000 == 0) {
//          ps.executeBatch()
//        }
//      }
//      ps.executeBatch()
//    } catch {
//      case exception: Exception =>
//        println(exception.printStackTrace())
//    }
//  }

  def executeSQL(connection: Connection,sql:String,rows:Array[Row],s: Seq[Int] = Seq()): Unit ={
    try{
      val ps = connection.prepareStatement(sql)
      var j = 0
      for (row <- rows){
        val it = row.schema.iterator
        var i = 0
        while (it.hasNext){
          it.next()
          ps.setObject(i + 1, row.get(i))
          i = i + 1
        }
        i = 0
        ps.addBatch()
        j = j + 1
        if (j % 500 == 0){
          ps.executeBatch()
          ps.clearBatch()
        }
      }
      ps.executeBatch()
    } catch {
      case exception: Exception => println(exception.printStackTrace())
    }

  }

  /**
   * 批量操作
   */
  def insertOrUpdateData(db_util: RdbmsUtil, res: DataFrame, db_nm: String, tb_nm: String,s: Seq[Int] = Seq(), sql_name:String): Unit = {
    val fields = res.columns
    val values = ("?," * fields.length).substring(0, ("?," * fields.length).lastIndexOf(","))
    println("schema=====================>")
    res.schema.foreach(println(_))
    println("schema=====================>")
    val data = res.rdd.collect()
//    val data = res.rdd.map(_.mkString("\u001B|").split("\u001B\\|")).collect()
    val conn = db_util.getConnection
    val f = fields.mkString(",")
    val v = new ListBuffer[String]
    for (elem <- fields.toList.tail if fields.toList.tail.nonEmpty) {
//      val value = elem+" = values("+elem+") "
      val value = elem+"=excluded."+elem
      v.append(value)
    }
    val duplicateKey = v.mkString(",")

//    val sql_str = s"insert into $db_nm.$tb_nm($f) values($values) on conflict (id) do update set $duplicateKey"
    val sql_str = s"insert into $tb_nm($f) values($values) on conflict (id) do update set $duplicateKey"
//    db_util.executeSQL(conn, sql_str, data, s )
    db_util.executeSQL(conn, sql_str, data, s )
  }


}
