package com.cloudminds.cdc.schema

import com.alibaba.fastjson.JSONObject
import com.cloudminds.cdc.model.source.MqttSourceModel
import org.apache.avro.Schema

import scala.collection.JavaConversions._
import java.util

class MqttSchemaFactory(model:MqttSourceModel) extends SchemaFactory {
  override def createSchema: util.HashMap[String, String] = {
    val tableSchema: util.HashMap[String, util.HashMap[String, String]] = model.tableSchema
    val mqttSchema = new util.HashMap[String, String]()
    for (kv <- tableSchema.entrySet){
      val schemaResults = new util.ArrayList[util.Map[String, Object]]
      val json = new JSONObject
      json.put("type", "record")
      json.put("name", kv.getKey)
      val v: util.HashMap[String, String] = kv.getValue
      // 封装处理字段信息
      for (kv2 <- v){
        val fieldMap = new util.HashMap[String, Object]()
        val tp: Array[String] = Array(kv2._2, "null")
        fieldMap.put("name",kv2._1)
        fieldMap.put("type",tp)
        schemaResults.add(fieldMap)

      }
      json.put("fields", schemaResults.toArray)
      val schema: Schema = new Schema.Parser().parse(json.toString)
      mqttSchema.put(kv.getKey,schema.toString)
    }
    mqttSchema
  }
}
