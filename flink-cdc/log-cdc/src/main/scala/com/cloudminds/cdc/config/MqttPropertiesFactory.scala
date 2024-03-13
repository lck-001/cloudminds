package com.cloudminds.cdc.config

import com.cloudminds.cdc.model.source.MqttSourceModel

import java.util

class MqttPropertiesFactory (confPath: String) extends PropertiesFactory[MqttSourceModel]{
  override def getProperties: MqttSourceModel = {
    val propMap: util.HashMap[String, Object] = PropertiesUtils.getProperties(confPath).get("source").asInstanceOf[util.HashMap[String,Object]]
    val dataEnv: String = Option(propMap.get("dataEnv")).getOrElse("bj-prod-232").asInstanceOf[String]
    val hostname: String = propMap.getOrDefault("hostname", "").asInstanceOf[String]
    val port: Int = propMap.getOrDefault("port", "1883").toString.toInt
    val queueSize: Int = propMap.getOrDefault("queueSize", "10").toString.toInt
    val username: String = Option(propMap.get("username")).getOrElse("").asInstanceOf[String]
    val password: String = Option(propMap.get("password")).getOrElse("").asInstanceOf[String]
    val topic: String = Option(propMap.get("topic")).getOrElse("").asInstanceOf[String]
    val qos: Int = propMap.getOrDefault("qos", "1").asInstanceOf[Int]
    val clientId: String = Option(propMap.get("clientId")).getOrElse("").asInstanceOf[String]
    val cleanSession: Boolean = Option(propMap.get("cleanSession")).getOrElse("false").asInstanceOf[Boolean]
    val connectionTimeout: Int = Option(propMap.get("connectionTimeout")).getOrElse("60").asInstanceOf[Int]
    val keepAliveInterval: Int = Option(propMap.get("keepAliveInterval")).getOrElse("120").asInstanceOf[Int]
    val tableSchemaList: util.ArrayList[util.HashMap[String, Object]] = propMap.get("tableSchema").asInstanceOf[util.ArrayList[util.HashMap[String, Object]]]
    val dataTuple: (util.HashMap[String, String], util.HashMap[String, util.HashMap[String, String]]) = getSchema(tableSchemaList)
    MqttSourceModel(dataEnv= dataEnv, hostname = hostname, port = port, username = username, password = password, topic = topic, qos = qos, clientId = clientId, cleanSession = cleanSession, connectionTimeout = connectionTimeout, keepAliveInterval = keepAliveInterval, queueSize = queueSize,tableRule = dataTuple._1, tableSchema = dataTuple._2)
  }
  private def getSchema(tableSchemaList: util.ArrayList[util.HashMap[String, Object]]):(util.HashMap[String,String],util.HashMap[String,util.HashMap[String,String]]) = {
    val tableRule = new util.HashMap[String,String]()
    val tableSchema = new util.HashMap[String,util.HashMap[String,String]]()
    if (null != tableSchemaList && !tableSchemaList.isEmpty){
      for (data <- tableSchemaList.toArray ){
        val dataMap: util.HashMap[String, Object] = data.asInstanceOf[util.HashMap[String, Object]]
        val dbTable : String = dataMap.get("dbTable").toString
        val rule: String = dataMap.get("rule").asInstanceOf[String]
        val schema: util.HashMap[String,String] = dataMap.get("schema").asInstanceOf[util.HashMap[String,String]]
        tableRule.put(dbTable,rule)
        tableSchema.put(dbTable,schema)
      }
    }
    (tableRule,tableSchema)
  }
}
