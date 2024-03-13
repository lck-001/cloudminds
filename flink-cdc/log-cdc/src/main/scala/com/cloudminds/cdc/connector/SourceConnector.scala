package com.cloudminds.cdc.connector

import com.cloudminds.cdc.transform.CustomerDeserializationSchema
import com.ververica.cdc.connectors.mysql.MySqlSource
import com.ververica.cdc.connectors.mysql.table.StartupOptions
import com.ververica.cdc.connectors.postgres.PostgreSQLSource
import com.ververica.cdc.debezium.{DebeziumSourceFunction, JsonDebeziumDeserializationSchema}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.api.java.utils.ParameterTool

import java.util
import java.util.Properties
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

object SourceConnector {
    // 测试环境
//  val propertiesFilePath = "/opt/soft/flink-1.13.5/job.properties"
  // 生产环境
  val propertiesFilePath = "/opt/soft/flink-1.13.6/job.properties"
  val parameter: ParameterTool = ParameterTool.fromPropertiesFile(propertiesFilePath)

  def getSourceType(prop:util.HashMap[String, Object]):SourceFunction[String] ={
    val sourceType = prop.get("sourceType").asInstanceOf[String]
    val source = sourceType match {
      case "postgres" => getPostgresSource(prop)
      case "mysql" => getMysqlSource(prop)
      case _ => null
    }
    source
  }

  def getPostgresSource[T: ClassTag](prop:util.HashMap[String, Object]) : SourceFunction[String]={
    val properties = new Properties()
    properties.setProperty("snapshot.mode","initial")
    val dbs = new ListBuffer[String]
    val sch = new ListBuffer[String]
    val tabs = new ListBuffer[String]
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
    val postgresCdcSource: DebeziumSourceFunction[String] = PostgreSQLSource
      .builder[String]
      .hostname(hostname)
      .port(port)
      .database(databases.mkString(","))
      .schemaList(schemas.mkString(","))
      .tableList(tables.mkString(","))
      .username(username)
      .password(password)
      //      .deserializer(new StringDebeziumDeserializationSchema())
      .deserializer(new JsonDebeziumDeserializationSchema(true))
      .slotName(slotName)
      //      .decodingPluginName("wal2json")
      .decodingPluginName("pgoutput")
      //      .debeziumProperties(properties)
      .build
    postgresCdcSource
  }
  def getMysqlSource[T: ClassTag](prop:util.HashMap[String, Object]) : SourceFunction[String]={
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
    val properties = new Properties()
    properties.setProperty("debezium.snapshot.locking.mode", "none")
    val mysqlSource = MySqlSource.builder[String]
      .hostname(hostname)
      .port(port)
      .deserializer(new JsonDebeziumDeserializationSchema(true)) //去参数里面找找实现类
//      .deserializer(new CustomerDeserializationSchema()) //去参数里面找找实现类
      .username(username)
      .password(password)
      .databaseList(databases.mkString(",")) // 指定某个特定的库
      .tableList(tables.mkString(",")) //指定特定的表
      .startupOptions(StartupOptions.latest()) // 读取binlog策略 这个启动选项有五种
      .debeziumProperties(properties) //配置不要锁表 但是数据一致性不是精准一次 会变成最少一次
      .build()
    mysqlSource.asInstanceOf[SourceFunction[String]]
  }
}
