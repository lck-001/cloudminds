/*
package com.cloudminds.cdc.transform

import com.alibaba.fastjson.{JSON, JSONObject}
import com.cloudminds.cdc.model.CommonSource
import com.cloudminds.cdc.utils.{JsonHandler, Placeholder}
import com.google.common.collect.{MapDifference, Maps}
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.{ConfigOption, ConfigOptions, Configuration}
import scala.util.control._
import scala.collection.JavaConversions._

import java.util

class ExtractMapFunction(sourceType: String) extends RichMapFunction[String,CommonSource]{
  private val kafkaSchema = new util.HashMap[String, (String, util.HashMap[String, String])]
  override def open(parameters: Configuration): Unit = {
    val globalJobParameters: ExecutionConfig.GlobalJobParameters = getRuntimeContext.getExecutionConfig.getGlobalJobParameters
    val globConf: Configuration = globalJobParameters.asInstanceOf[Configuration]
    val kafkaSourceCfg: ConfigOption[util.Map[String, String]] = ConfigOptions.key("kafkaSourceCfg").mapType().noDefaultValue()
    val kafkaCfgMap: util.Map[String, String] = globConf.get(kafkaSourceCfg)
    for (kv <- kafkaCfgMap ){
      val ruleSchemaArr: Array[String] = kv._2.split("###")
      val schemaMap: util.HashMap[String, String] = JSON.parseObject(ruleSchemaArr.last, classOf[util.HashMap[String, String]])
      kafkaSchema.put(kv._1.replaceAll("`",""),(ruleSchemaArr.head,schemaMap))
    }
  }
  override def map(in: String): CommonSource = {
    if ("kafka".equalsIgnoreCase(sourceType)){
      var dataTable = ""
      val dataJson = new JSONObject()
      val smp = new util.HashMap[String,String]()
      Breaks.breakable{
        for (kv <- kafkaSchema){
          val rule: String = kv._2._1
          val flatMap = new util.HashMap[String, Object]()
          JsonHandler.analysisJson(in,"",flatMap)
          // 获取匹配规则
          val dbTable: String = Placeholder.replace(rule, flatMap, "${", "}", false)
          // 获取对应的 kafka schema
          if (kv._1.equalsIgnoreCase(dbTable)){
            // 过滤出需要的字段
            getColumns(flatMap,kv._2._2,dataJson)
            dataTable = dbTable
            smp.putAll(kv._2._2)
            Breaks.break()
          }
        }
      }
      CommonSource(db = "", table = dataTable, bigdata_method = "c", event_time = System.currentTimeMillis(), schema = smp, before = null, after = dataJson)
    }else{
      val jsonObject = JSON.parseObject(in)
      val payload = jsonObject.getJSONObject("payload")
      val source = payload.getJSONObject("source")
      // 获取数据库
      val db = source.getString("db")
      // 获取表
      val table = source.getString("table")
      // 操作类型
      val op = payload.getString("op")
      // 操作时间
      val ts_ms = payload.getLong("ts_ms")
      // 获取schema
      val schema = getSchema(op, jsonObject)
      // before
      val before = payload.getJSONObject("before")
      // after
      val after = payload.getJSONObject("after")

      CommonSource(db = db, table = table, bigdata_method = op, event_time = ts_ms, schema = schema, before = before, after = after)
    }
  }

  def getSchema(op:String,json:JSONObject):util.HashMap[String, String]={
    val schema = json.getJSONObject("schema")
    val fields = schema.getJSONArray("fields")

    // 删除状态 读取before 否则读取after
    if ("d".equalsIgnoreCase(op)){
      val before = fields.getJSONObject(0)
      val fieldMap = getJsonArr(before)
      fieldMap
    }else{
      val after = fields.getJSONObject(1)
      val fieldMap = getJsonArr(after)
      fieldMap
    }
  }

  def getJsonArr(json:JSONObject):util.HashMap[String, String]={
    val fieldMap = new util.HashMap[String, String]()
    val fields = json.getJSONArray("fields")
    for (arr <- fields.toArray){
      val fieldObj = arr.asInstanceOf[JSONObject]
      val field = fieldObj.getString("field")
      val fieldType = fieldObj.getString("type")
      fieldMap.put(field,fieldType)
    }
    fieldMap
  }


  def getColumns(dataMap:util.Map[String, Object],schema:util.HashMap[String, String],dataJson:JSONObject):Unit={
    val difference: MapDifference[String, Object] = Maps.difference(dataMap, schema)
    val schemaData: util.Map[String, MapDifference.ValueDifference[Object]] = difference.entriesDiffering()
    for(entry <- schemaData){
      dataJson.put(entry._1,entry._2.leftValue())
    }
  }
}
*/
