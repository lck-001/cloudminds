package com.cloudminds.cdc.connector

import com.cloudminds.cdc.config.PropertiesConfig
import com.cloudminds.cdc.config.PropertiesHandler.prop
import com.cloudminds.cdc.model.source.MysqlSourceModel
import com.ververica.cdc.connectors.mysql.source.MySqlSourceBuilder
import java.util.Optional

import com.cloudminds.cdc.transform.CustomerDeserializationSchema
//import com.ververica.cdc.connectors.mysql.MySqlSource
import com.ververica.cdc.connectors.mysql.source.MySqlSource
import com.ververica.cdc.connectors.mysql.table.StartupOptions
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction

import java.util
import java.util.Properties
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

object MysqlConnector {
  def getMysqlSource[T: ClassTag](mysqlModel: MysqlSourceModel) : MySqlSource[String]={
    val properties = new Properties()
    if (null != mysqlModel.snapshotMode ){
      properties.setProperty("snapshot.mode", mysqlModel.snapshotMode.getValue)
    }
    properties.setProperty("snapshot.locking.mode", "none")
    properties.setProperty("decimal.handling.mode", "double")
    properties.setProperty("event.deserialization.failure.handling.mode", "warn")
    //    properties.setProperty("debezium.snapshot.locking.mode", "none")
    val jdbcProperties = new Properties()
    jdbcProperties.setProperty("useSSL", "false")
    val builder: MySqlSourceBuilder[String] = MySqlSource.builder[String]
      .hostname(mysqlModel.hostname)
      .port(mysqlModel.port)
//      .deserializer(new JsonDebeziumDeserializationSchema(true)) //去参数里面找找实现类
            .deserializer(new CustomerDeserializationSchema()) //去参数里面找找实现类
      .username(mysqlModel.username)
      .password(mysqlModel.password)
      .databaseList(mysqlModel.databases.mkString(",").replaceAll("`", "")) // 指定某个特定的库
      .tableList(mysqlModel.dbTables.mkString(",").replaceAll("`", "")) //指定特定的表
      .scanNewlyAddedTableEnabled(true)
      //      .startupOptions(StartupOptions.initial()) // 读取binlog策略 这个启动选项有五种//
      //      .startupOptions(getStartupOptions(config.startupOptions)) // 读取binlog策略 这个启动选项有五种
      .debeziumProperties(properties) //配置不要锁表 但是数据一致性不是精准一次 会变成最少一次
      .jdbcProperties(jdbcProperties) //.splitSize(8096*2)
    val serverId: String = mysqlModel.serverId
    if (null != serverId && serverId.nonEmpty){
      builder.serverId(serverId)
    }
    val mysqlSource: MySqlSource[String] = builder.build()
    mysqlSource
  }
/*  def getMysqlSource[T: ClassTag](prop:util.HashMap[String, Object]) : MySqlSource[String]={
    val config = PropertiesConfig.getConfig(prop)
    val properties = new Properties()
    properties.setProperty("snapshot.locking.mode", "none")
    properties.setProperty("decimal.handling.mode", "double")
//    properties.setProperty("debezium.snapshot.locking.mode", "none")
    val jdbcProperties = new Properties()
    jdbcProperties.setProperty("useSSL", "false")
    val mysqlSource = MySqlSource.builder[String]
      .hostname(config.hostname)
      .port(config.port)
      .deserializer(new JsonDebeziumDeserializationSchema(true)) //去参数里面找找实现类
      //      .deserializer(new CustomerDeserializationSchema()) //去参数里面找找实现类
      .username(config.username)
      .password(config.password)
      .databaseList(config.databases.mkString(",").replaceAll("`","")) // 指定某个特定的库
      .tableList(config.tables.mkString(",").replaceAll("`","")) //指定特定的表
      .scanNewlyAddedTableEnabled(true)
//      .startupOptions(StartupOptions.initial()) // 读取binlog策略 这个启动选项有五种
      .startupOptions(getStartupOptions(config.startupOptions)) // 读取binlog策略 这个启动选项有五种
      .debeziumProperties(properties) //配置不要锁表 但是数据一致性不是精准一次 会变成最少一次
      .jdbcProperties(jdbcProperties)//.splitSize(8096*2)
      .build()
    mysqlSource
  }
  def getStartupOptions(startOptions:util.HashMap[String,String]):StartupOptions ={
    val startType = Option(startOptions).getOrElse(new util.HashMap[String,String]).getOrDefault("startType","").toLowerCase
    val startupOptions = startType match {
      case "initial" => StartupOptions.initial()
      case "earliest_offset" => StartupOptions.earliest()
      case "latest_offset" => StartupOptions.latest()
      case "timestamp" => {
        try{
          val offset = startOptions.get("offset")
          StartupOptions.timestamp(offset.toLong)
        }catch{
          case e:Exception => throw new Exception("please input offset !!!")
        }
      }
      case "specific_offsets" =>{
        val filename = startOptions.getOrDefault("filename", "")
        try{
          if (filename.nonEmpty){
            val offset = startOptions.get("offset").asInstanceOf[Int]
            StartupOptions.specificOffset(filename,offset)
          }else{
            throw new Exception("please input filename !!!")
          }
        }catch {
          case e:Exception => throw new Exception("please input offset !!!")
        }
      }
      case _ => StartupOptions.initial()
    }
    startupOptions
  }*/
}