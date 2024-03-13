package com.cloudminds.cdc.transform

import com.alibaba.fastjson.{JSON, JSONObject}
import com.cloudminds.cdc.model.Project
import org.apache.flink.api.common.functions.RichMapFunction

class ProjectMapFunction extends RichMapFunction[String,Project]{
  override def map(record: String): Project = {
    val json = JSON.parseObject(record, classOf[JSONObject])
    val op = json.getOrDefault("op", "").toString
    val ts_ms = json.getLongValue("ts_ms")
    val source = json.getJSONObject("source")
    val db = source.getString("db")
    val table = source.getString("table")
    if (op.equalsIgnoreCase("d")){
      val before = json.getObject("before", classOf[JSONObject])
      val project = getRecord(before, op, ts_ms,db, table)
      project
    }else{
      val after = json.getObject("after", classOf[JSONObject])
      val project = getRecord(after, op, ts_ms,db, table)
      project
    }
  }

  def getRecord(json : JSONObject,op:String,ts_ms:Long,db:String,table:String):Project ={
    val id = json.getIntValue("id")

    val created = if(json.getOrDefault("created", "") != null) json.getOrDefault("created", "").toString else ""
    val updated = if(json.getOrDefault("updated", "") != null) json.getOrDefault("updated", "").toString else ""
    val status = json.getIntValue("status")
    val name = if(json.getOrDefault("name", "") != null) json.getOrDefault("name", "").toString else ""
    val priority = if(json.getOrDefault("priority", "") != null) json.getOrDefault("priority", "").toString else ""
    val import_date = if(json.getOrDefault("import_date", "") != null) json.getOrDefault("import_date", "").toString else ""
    val start_date = if(json.getOrDefault("start_date", "") != null ) json.getOrDefault("start_date", "").toString else ""
    val end_date = if(json.getOrDefault("end_date", "") != null) json.getOrDefault("end_date", "").toString else ""
    val category = if(json.getOrDefault("category", "") != null) json.getOrDefault("category", "").toString else ""
    val industry = if(json.getOrDefault("industry", "") != null) json.getOrDefault("industry", "").toString else ""
    val trainer = json.getIntValue("trainer")
    val trainer_name = if(json.getOrDefault("trainer_name", "") != null) json.getOrDefault("trainer_name", "").toString else ""
    val operator = json.getIntValue("operator")
    val operator_name = if(json.getOrDefault("operator_name", "") != null) json.getOrDefault("operator_name", "").toString else ""
    val content_operator = json.getIntValue("content_operator")
    val content_operator_name = if(json.getOrDefault("content_operator_name", "") != null) json.getOrDefault("content_operator_name", "").toString else ""
    val agents = if(json.getOrDefault("agents", "") != null) json.getOrDefault("agents", "").toString else ""
    Project(id , created , updated , status, name , priority , import_date , start_date, end_date , category , industry , trainer , trainer_name , operator , operator_name , content_operator , content_operator_name , agents , ts_ms , op, db, table)
  }

}
