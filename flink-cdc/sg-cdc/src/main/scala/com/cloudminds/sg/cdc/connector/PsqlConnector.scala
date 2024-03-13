package com.cloudminds.sg.cdc.connector

import com.cloudminds.sg.cdc.config.PropertiesConfig
import com.ververica.cdc.connectors.postgres.PostgreSQLSource
import com.ververica.cdc.debezium.{DebeziumSourceFunction, JsonDebeziumDeserializationSchema}
import org.apache.flink.streaming.api.functions.source.SourceFunction

import java.util
import java.util.Properties
import scala.reflect.ClassTag

object PsqlConnector {
  def getPostgresSource[T: ClassTag](prop:util.HashMap[String, Object]) : SourceFunction[String]={
    val config = PropertiesConfig.getConfig(prop)
    val properties = new Properties()
    properties.setProperty("snapshot.mode","initial")
    val postgresCdcSource: DebeziumSourceFunction[String] = PostgreSQLSource
      .builder[String]
      .hostname(config.hostname)
      .port(config.port)
      .database(config.databases.mkString(","))
      .schemaList(config.schemas.mkString(","))
      .tableList(config.tables.mkString(","))
      .username(config.username)
      .password(config.password)
      //      .deserializer(new StringDebeziumDeserializationSchema())
      .deserializer(new JsonDebeziumDeserializationSchema(true))
      .slotName(config.slotName)
      //      .decodingPluginName("wal2json")
      .decodingPluginName("pgoutput")
      //      .debeziumProperties(properties)
      .build
    postgresCdcSource
  }
}
