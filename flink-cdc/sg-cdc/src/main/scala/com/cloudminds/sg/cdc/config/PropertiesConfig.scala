package com.cloudminds.sg.cdc.config

import com.cloudminds.sg.cdc.model.JdbcConfig

import java.util
import scala.collection.mutable.ListBuffer

object PropertiesConfig {
  def getConfig(prop:util.HashMap[String, Object]):JdbcConfig={
    val sourceType = prop.get("sourceType").asInstanceOf[String]
    val config = sourceType match {
      case "postgres" => getPostgresProp(prop)
      case "mysql" => getMysqlProp(prop)
      case "mongodb" => getMongoProp(prop)
      case _ => null
    }
    config
  }
  def getPostgresProp(prop: util.HashMap[String, Object]): JdbcConfig ={
    val dbs = new ListBuffer[String]
    val sch = new ListBuffer[String]
    val tabs = new ListBuffer[String]
    val dbTab = new ListBuffer[String]
    val dataList = prop.get("data").asInstanceOf[util.ArrayList[util.HashMap[String,String]]]
    for (data <- dataList.toArray){
      val dataMap = data.asInstanceOf[util.HashMap[String, String]]
      val database = dataMap.getOrDefault("database","")
      dbs.append(database)
      val schema = dataMap.getOrDefault("schema","")
      sch.append(schema)
      val tableList = dataMap.getOrDefault("tables","")
      for (table <- tableList.split(",")){
        if (table.nonEmpty){
          tabs.append(schema+"."+table)
          dbTab.append(database+"."+table)
        }
      }
    }
    val databases = dbs.filter(s => s.nonEmpty)
    val schemas = sch.filter(s => s.nonEmpty)
    val tables = tabs.filter(s => s.nonEmpty)
    val hostname = prop.getOrDefault("hostname","").toString
    val port = prop.get("port").asInstanceOf[Int]
    val username = prop.getOrDefault("username","").toString
    val password = prop.getOrDefault("password","").toString
    val slotName = prop.getOrDefault("slotName","").toString
    JdbcConfig("postgres",databases,schemas,tables,hostname,port,username,password,slotName,dbTab)
  }
  def getMysqlProp(prop: util.HashMap[String, Object]): JdbcConfig ={
    val dbs = new ListBuffer[String]
    val tabs = new ListBuffer[String]
    val dataList = prop.get("data").asInstanceOf[util.ArrayList[util.HashMap[String,String]]]
    for (data <- dataList.toArray){
      val dataMap = data.asInstanceOf[util.HashMap[String, String]]
      val database = dataMap.getOrDefault("database","")
      dbs.append(database)
      val tableList = dataMap.getOrDefault("tables","")
      for (table <- tableList.split(",")){
        if (table.nonEmpty){
          tabs.append(database+"."+table)
        }
      }
    }
    val databases = dbs.filter(s => s.nonEmpty)
    val tables = tabs.filter(s => s.nonEmpty)
    val hostname = prop.getOrDefault("hostname","").toString
    val port = prop.get("port").asInstanceOf[Int]
    val username = prop.getOrDefault("username","").toString
    val password = prop.getOrDefault("password","").toString
    JdbcConfig("mysql",databases,null,tables,hostname,port,username,password,"",tables)
  }

  def getMongoProp(prop: util.HashMap[String, Object]): JdbcConfig ={
    val dbs = new ListBuffer[String]
    val tabs = new ListBuffer[String]
    val dataList = prop.get("data").asInstanceOf[util.ArrayList[util.HashMap[String,String]]]
    for (data <- dataList.toArray){
      val dataMap = data.asInstanceOf[util.HashMap[String, String]]
      val database = dataMap.getOrDefault("database","")
      dbs.append(database)
      val tableList = dataMap.getOrDefault("tables","")
      for (table <- tableList.split(",")){
        if (table.nonEmpty){
          tabs.append(database+"."+table)
        }
      }
    }
    val databases = dbs.filter(s => s.nonEmpty)
    val tables = tabs.filter(s => s.nonEmpty)
    val hostname = prop.getOrDefault("hostname","").toString
    val port = prop.get("port").asInstanceOf[Int]
    val username = prop.getOrDefault("username","").toString
    val password = prop.getOrDefault("password","").toString
    JdbcConfig("mongodb",databases,null,tables,hostname,port,username,password,"",tables)
  }
}
