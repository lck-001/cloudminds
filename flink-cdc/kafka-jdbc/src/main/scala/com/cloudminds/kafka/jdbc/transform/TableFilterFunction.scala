package com.cloudminds.kafka.jdbc.transform

import com.alibaba.fastjson.{JSON, JSONObject}
import com.cloudminds.kafka.jdbc.utils.TableType
import org.apache.flink.api.common.functions.RichFilterFunction

import java.util

class TableFilterFunction(schemaTable:util.HashMap[String, util.HashMap[String, String]]) extends RichFilterFunction[String]{
  override def filter(in: String): Boolean = {
    val jsonObject = JSON.parseObject(in)
    val tableType: String = TableType.getTableType(jsonObject)
    val payload = jsonObject.getJSONObject("payload")
    var db = ""
    var table = ""
    if ("mongo".equalsIgnoreCase(tableType)){
      val ns: JSONObject = payload.getJSONObject("ns")
      db = ns.getString("db")
      table = ns.getString("coll")
    }else{
      val source = payload.getJSONObject("source")
      // 获取数据库
      db = source.getString("db")
      // 获取表
      table = source.getString("table")
    }
    val schemaColumns: util.HashMap[String, String] = schemaTable.get(db + "." + table)
    if (schemaColumns != null){
      true
    }else{
      false
    }
  }
}
