package com.cloudminds.cdc.schema

import com.alibaba.fastjson.JSONObject
import com.cloudminds.cdc.model.source.MongoSourceModel

import scala.collection.JavaConversions._
import org.apache.avro.Schema

import java.util

class MongoSchemaFactory(model:MongoSourceModel) extends SchemaFactory {
  /**
   * 返回schema map
   * @return tableName,schemaStr
   */
  override def createSchema: util.HashMap[String, String] = {
    val tableSchema: util.HashMap[String, util.HashMap[String, String]] = model.tableSchema
    val MongoSchema = new util.HashMap[String, String]()
    for (kv <- tableSchema.entrySet){
      val schemaResults = new util.ArrayList[util.Map[String, Object]]
      val json = new JSONObject
      json.put("type", "record")
      json.put("name", kv.getKey)
      val v: util.HashMap[String, String] = kv.getValue
      // 封装处理字段信息
      for (kv2 <- v){
        val name: String = kv2._1.replaceAll("\\.", "_P_").replaceAll("\\$","_D_")
        val fieldMap = new util.HashMap[String, Object]()
        val tp: Array[String] = Array(kv2._2, "null")
        fieldMap.put("name",name)
        fieldMap.put("type",tp)
        schemaResults.add(fieldMap)

      }
      json.put("fields", schemaResults.toArray)
      val schema: Schema = new Schema.Parser().parse(json.toString)
      MongoSchema.put(kv.getKey,schema.toString)
    }
    MongoSchema
  }
}
