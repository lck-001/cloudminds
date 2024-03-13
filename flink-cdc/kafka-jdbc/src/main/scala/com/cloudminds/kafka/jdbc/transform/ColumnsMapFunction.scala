package com.cloudminds.kafka.jdbc.transform

import com.alibaba.fastjson.serializer.SerializerFeature
import com.alibaba.fastjson.{JSON, JSONObject}
import com.cloudminds.kafka.jdbc.utils.{OplogHandler, TableType}
import com.github.wnameless.json.flattener.JsonFlattener
import com.google.common.collect.{MapDifference, Maps}
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.{ConfigOption, ConfigOptions, Configuration}

import scala.collection.JavaConversions._
import java.util

class ColumnsMapFunction(schemaTable:util.HashMap[String, util.HashMap[String, String]]) extends RichMapFunction[String,JSONObject]{
  var k8s_env_name = ""
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    val globalJobParameters: ExecutionConfig.GlobalJobParameters = getRuntimeContext.getExecutionConfig.getGlobalJobParameters
    val globConf: Configuration = globalJobParameters.asInstanceOf[Configuration];
    k8s_env_name = globConf.getString(ConfigOptions.key("k8s_env_name").stringType().defaultValue("bj-prod-232"))
  }
  override def map(in: String): JSONObject = {
    val jsonObject = JSON.parseObject(in)
    val tableType: String = TableType.getTableType(jsonObject)
    val dataJson = new JSONObject()
    val payload = jsonObject.getJSONObject("payload")
    val source = payload.getJSONObject("source")
    if ("mongo".equalsIgnoreCase(tableType)){
      val ns: JSONObject = payload.getJSONObject("ns")
      val db = ns.getString("db")
      val table = ns.getString("coll")
      val operation: String = payload.getString("operationType")
      val bigdata_method = operation.toLowerCase match {
        case "insert" => "c"
        case "update" => "u"
        case "delete" => "d"
        case _ => "c"
      }
      dataJson.put("db",db)
      dataJson.put("table",table)
      dataJson.put("bigdata_method",bigdata_method)
      dataJson.put("k8s_env_name",k8s_env_name)
      val schemaColumns: util.HashMap[String, String] = schemaTable.get(db + "." + table)
      if (!schemaColumns.isEmpty && schemaColumns != null){
        var fullDocument: String = payload.getString("fullDocument")
//        val fullMap: util.Map[String, Object] = JsonFlattener.flattenAsMap(fullDocument)
        val fullMap = new util.HashMap[String, Object]()
/*        val fullJson: JSONObject = JSON.parseObject(fullDocument)
        for (entry <- fullJson.entrySet()){
          fullMap.put(entry.getKey,entry.getValue)
        }*/
        if (null == fullDocument ){
          fullDocument = new JSONObject().toString
        }
        OplogHandler.analysisJson(fullDocument,"",fullMap)
        getColumns(fullMap,schemaColumns,dataJson)
        dataJson
      }else{
        throw new RuntimeException("schema initial failed !!!")
      }
    }else{
      // 获取数据库
      val db = source.getString("db")
      // 获取表
      val table = source.getString("table")
      val schemaColumns: util.HashMap[String, String] = schemaTable.get(db + "." + table)
      if (!schemaColumns.isEmpty && schemaColumns != null){
        // 操作类型
        val op = payload.getString("op")
        // 操作时间
        //      val ts_ms = payload.getLong("ts_ms")

        dataJson.put("db",db)
        dataJson.put("table",table)
        dataJson.put("bigdata_method",op)
        dataJson.put("k8s_env_name",k8s_env_name)

        if (op != null && "d".equalsIgnoreCase(op)){
          // before
          val before = payload.getJSONObject("before")
          val dataMap: util.HashMap[String, Object] = JSON.parseObject(JSON.toJSONString(before,SerializerFeature.WriteMapNullValue), classOf[util.HashMap[String, Object]])
          getColumns(dataMap,schemaColumns,dataJson)
        }else{
          // after
          val after = payload.getJSONObject("after")
          val dataMap: util.HashMap[String, Object] = JSON.parseObject(JSON.toJSONString(after,SerializerFeature.WriteMapNullValue), classOf[util.HashMap[String, Object]])
          getColumns(dataMap,schemaColumns,dataJson)
        }
        dataJson
      }else{
        throw new RuntimeException("schema initial failed !!!")
      }
    }
  }
  def getColumns(dataMap:util.Map[String, Object],schema:util.HashMap[String, String],dataJson:JSONObject):Unit={
    val difference: MapDifference[String, Object] = Maps.difference(dataMap, schema)
    val schemaData: util.Map[String, MapDifference.ValueDifference[Object]] = difference.entriesDiffering()
    for(entry <- schemaData){
      dataJson.put(entry._1,entry._2.leftValue())
    }
/*    schemaData.forEach({
      case (key:String,value:MapDifference.ValueDifference[Object]) => dataJson.put(key,value.leftValue())
    })*/
  }
}
