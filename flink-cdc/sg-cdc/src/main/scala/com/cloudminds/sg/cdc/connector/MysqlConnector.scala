package com.cloudminds.sg.cdc.connector

import com.cloudminds.sg.cdc.config.PropertiesConfig
import com.ververica.cdc.connectors.mysql.source.MySqlSource
import com.ververica.cdc.connectors.mysql.table.StartupOptions
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema

import java.util
import java.util.Properties
import scala.reflect.ClassTag

object MysqlConnector {
  def getMysqlSource[T: ClassTag](prop:util.HashMap[String, Object]) : MySqlSource[String]={
    val config = PropertiesConfig.getConfig(prop)
    val properties = new Properties()
    properties.setProperty("debezium.snapshot.locking.mode", "none")
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
      .databaseList(config.databases.mkString(",")) // 指定某个特定的库
      .tableList(config.tables.mkString(",")) //指定特定的表
//      .startupOptions(StartupOptions.initial()) // 读取binlog策略 这个启动选项有五种
      .startupOptions(StartupOptions.initial()) // 读取binlog策略 这个启动选项有五种
      .debeziumProperties(properties) //配置不要锁表 但是数据一致性不是精准一次 会变成最少一次
      .scanNewlyAddedTableEnabled(true)
      .jdbcProperties(jdbcProperties)
      .build()
    mysqlSource
  }
}
