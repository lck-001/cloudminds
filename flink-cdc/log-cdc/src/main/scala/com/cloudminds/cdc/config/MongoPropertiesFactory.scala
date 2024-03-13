package com.cloudminds.cdc.config

import com.cloudminds.cdc.model.source.MongoSourceModel

import java.util
import scala.collection.mutable.ListBuffer

class MongoPropertiesFactory(confPath: String) extends PropertiesFactory[MongoSourceModel]{
  override def getProperties: MongoSourceModel = {
    val propMap: util.HashMap[String, Object] = PropertiesUtils.getProperties(confPath).get("source").asInstanceOf[util.HashMap[String,Object]]
    val dataEnv: String = Option(propMap.get("dataEnv")).getOrElse("bj-prod-232").asInstanceOf[String]
    val hostname: String = propMap.getOrDefault("hostname", "").asInstanceOf[String]
    val port: Int = propMap.getOrDefault("port", "27017").asInstanceOf[Int]
    val username: String = Option(propMap.get("username")).getOrElse("").asInstanceOf[String]
    val password: String = Option(propMap.get("password")).getOrElse("").asInstanceOf[String]
    val tableSchemaList: util.ArrayList[util.HashMap[String, Object]] = propMap.get("tableSchema").asInstanceOf[util.ArrayList[util.HashMap[String, Object]]]
    val dataList: util.ArrayList[util.HashMap[String, String]] = propMap.get("data").asInstanceOf[util.ArrayList[util.HashMap[String, String]]]
    val tableSchema: util.HashMap[String, util.HashMap[String, String]] = getSchema(tableSchemaList)
    val dataTuple: (ListBuffer[String], ListBuffer[String]) = getDbTables(dataList)
    MongoSourceModel(dataEnv = dataEnv,hostname = hostname, port = port, username = username, password = password, databases = dataTuple._1, dbTables = dataTuple._2, tableSchema = tableSchema)
  }
  private def getSchema(tableSchemaList: util.ArrayList[util.HashMap[String, Object]]): util.HashMap[String, util.HashMap[String, String]] ={
    val tableSchema = new util.HashMap[String,util.HashMap[String,String]]()
    if (null != tableSchemaList && !tableSchemaList.isEmpty) {
      for (data <- tableSchemaList.toArray ){
        val dataMap: util.HashMap[String, Object] = data.asInstanceOf[util.HashMap[String, Object]]
        val dbTable: String = dataMap.get("dbTable").asInstanceOf[String]
        val schema: util.HashMap[String,String] = dataMap.get("schema").asInstanceOf[util.HashMap[String,String]]
        tableSchema.put(dbTable,schema)
      }
    }
    tableSchema
  }
  private def getDbTables(dataList: util.ArrayList[util.HashMap[String, String]]): (ListBuffer[String], ListBuffer[String]) ={
    val dbs = new ListBuffer[String]
    val dbTabs = new ListBuffer[String]
    val tables = new ListBuffer[String]
    for (data <- dataList.toArray) {
      val dataMap = data.asInstanceOf[util.HashMap[String, String]]
      val database = dataMap.getOrDefault("database", "")
      dbs.append(database)
      val tableList = dataMap.getOrDefault("tables", "")
      for (table <- tableList.split(",")) {
        if (table.nonEmpty) {
          // tabs 已经拼接成 db.table
          dbTabs.append(database + "." + table)
          tables.append(table)
        }
      }
    }
    val databases = dbs.filter(s => s.nonEmpty)
    val dbTables: ListBuffer[String] = dbTabs.filter(s => s.nonEmpty)
    (databases, dbTables)
  }
}
