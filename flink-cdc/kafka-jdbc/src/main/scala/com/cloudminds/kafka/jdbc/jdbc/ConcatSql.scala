package com.cloudminds.kafka.jdbc.jdbc

import java.util
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

object ConcatSql {
  def getSql(url:String,params: util.HashMap[String,Object]): String={
    val sinkDatabase: String = params.getOrDefault("sinkDatabase", "").toString
    val sinkTable: String = params.getOrDefault("sinkTable", "").toString
    val columns: util.ArrayList[String] = params.get("columns").asInstanceOf[util.ArrayList[String]]
    val columnStr: String = String.join(",", columns)
    val arr: Array[String] = Array[String]()
    val place: String = arr.padTo(columns.size(), "?").mkString(",")
    var sql = s"insert into $sinkDatabase.$sinkTable($columnStr) values($place)"
    if (url.nonEmpty && url != null){
      val urlType: String = url.split(":")(1)
      urlType.toLowerCase match {
        case "mysql" => {
          val dv = new ListBuffer[String]
          for (elem <- columns.toList.tail if columns.toList.tail.nonEmpty) {
            val value = elem+" = values("+elem+") "
            dv.append(value)
          }
          val duplicateKey = dv.mkString(",")
          sql=sql+s" ON DUPLICATE KEY UPDATE $duplicateKey"
          sql
        }
        case "postgresql" => {
          val dv = new ListBuffer[String]
          for (elem <- columns.toList.tail if columns.toList.tail.nonEmpty) {
            val value: String = elem+"=excluded."+elem
            dv.append(value)
          }
          val conflictKey: String = columns.get(0)
          val duplicateKey: String = dv.mkString(",")
          sql=sql+s" on conflict ($conflictKey) do update set $duplicateKey"
          sql
        }
        case _ => sql
      }
    }else{
      throw new Exception("no sink url set !!!")
    }
  }
}
