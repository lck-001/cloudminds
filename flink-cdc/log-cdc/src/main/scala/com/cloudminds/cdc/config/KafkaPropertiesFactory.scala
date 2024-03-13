package com.cloudminds.cdc.config

import com.cloudminds.cdc.model.source
import com.cloudminds.cdc.model.source.KafkaSourceModel

import java.util

class KafkaPropertiesFactory(confPath:String) extends PropertiesFactory[KafkaSourceModel]{
  override def getProperties: KafkaSourceModel = {
    val propMap: util.HashMap[String, Object] = PropertiesUtils.getProperties(confPath).get("source").asInstanceOf[util.HashMap[String,Object]]
    val dataEnv: String = Option(propMap.get("dataEnv")).getOrElse("bj-prod-232").asInstanceOf[String]
    val bootstrapServers: String = propMap.getOrDefault("bootstrapServers", "").asInstanceOf[String]
    val topic: String = propMap.getOrDefault("topic", "").asInstanceOf[String]
    val groupId: String = propMap.getOrDefault("groupId", "").asInstanceOf[String]
    val startingOffsets: String = Option(propMap.get("startingOffsets")).getOrElse("").asInstanceOf[String]
    val tableSchemaList: util.ArrayList[util.HashMap[String, Object]] = propMap.get("tableSchema").asInstanceOf[util.ArrayList[util.HashMap[String, Object]]]
    val dataTuple: (util.HashMap[String, String], util.HashMap[String, util.HashMap[String, String]]) = getSchema(tableSchemaList)
    KafkaSourceModel(dataEnv = dataEnv, bootstrapServers = bootstrapServers, topic = topic, groupId = groupId, startingOffsets = startingOffsets, tableRule = dataTuple._1, tableSchema = dataTuple._2)
  }

  private def getSchema(tableSchemaList: util.ArrayList[util.HashMap[String, Object]]):(util.HashMap[String,String],util.HashMap[String,util.HashMap[String,String]]) = {
    val tableRule = new util.HashMap[String,String]()
    val tableSchema = new util.HashMap[String,util.HashMap[String,String]]()
    if (null != tableSchemaList && !tableSchemaList.isEmpty){
      for (data <- tableSchemaList.toArray ){
        val dataMap: util.HashMap[String, Object] = data.asInstanceOf[util.HashMap[String, Object]]
        val dbTable: String = dataMap.get("dbTable").asInstanceOf[String]
        val rule: String = dataMap.get("rule").asInstanceOf[String]
        val schema: util.HashMap[String,String] = dataMap.get("schema").asInstanceOf[util.HashMap[String,String]]
        tableRule.put(dbTable,rule)
        tableSchema.put(dbTable,schema)
      }
    }
    (tableRule,tableSchema)
  }
}
