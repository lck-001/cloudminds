package com.cloudminds.kafka.jdbc.utils

import com.alibaba.fastjson.JSONObject

object TableType {
  def getTableType(json:JSONObject):String ={
    val jsonSchema: JSONObject = json.getJSONObject("schema")
    val name: String = jsonSchema.getString("name")
    val tableType = name.split("\\.").head.toLowerCase match {
      case "mysql_binlog_source" => "mysql"
      case "changestream" => "mongo"
      case "postgres_cdc_source" => "postgre"
      case _ => ""
    }
    tableType
  }
}
