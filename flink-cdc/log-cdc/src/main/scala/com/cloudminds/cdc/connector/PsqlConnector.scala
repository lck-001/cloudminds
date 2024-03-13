package com.cloudminds.cdc.connector

import com.cloudminds.cdc.config.PropertiesConfig
import com.cloudminds.cdc.model.source.PsqlSourceModel
import com.ververica.cdc.connectors.postgres.PostgreSQLSource
import com.ververica.cdc.debezium.{DebeziumSourceFunction, JsonDebeziumDeserializationSchema}
import org.apache.flink.streaming.api.functions.source.SourceFunction

import java.util
import java.util.Properties
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

object PsqlConnector {
  def getPostgresSource[T: ClassTag](psqlModel: PsqlSourceModel) : SourceFunction[String]={
    val properties = new Properties()
    if (null != psqlModel.snapshotMode) {
      properties.setProperty("snapshot.mode",psqlModel.snapshotMode.getValue.toLowerCase)
    }
    //    properties.setProperty("debezium.slot.drop.on.stop","true") //停止的时候删除复制槽
    val postgresCdcSource: DebeziumSourceFunction[String] = PostgreSQLSource
      .builder[String]
      .hostname(psqlModel.hostname)
      .port(psqlModel.port)
      .database(psqlModel.databases.mkString(","))
      .schemaList(psqlModel.schemas.mkString(","))
      .tableList(psqlModel.schemaTables.mkString(","))
      .username(psqlModel.username)
      .password(psqlModel.password)
      //      .deserializer(new StringDebeziumDeserializationSchema())
      .deserializer(new JsonDebeziumDeserializationSchema(true))
      .slotName(psqlModel.slotName)
      //      .decodingPluginName("wal2json")
      .decodingPluginName("pgoutput")
      //      .debeziumProperties(properties)
      .build
    postgresCdcSource
  }
/*  def getPostgresSource[T: ClassTag](prop:util.HashMap[String, Object]) : SourceFunction[String]={
    val config = PropertiesConfig.getConfig(prop)
    val properties = new Properties()
    properties.setProperty("snapshot.mode","initial")
//    properties.setProperty("debezium.slot.drop.on.stop","true") //停止的时候删除复制槽
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
  }*/
}
