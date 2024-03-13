package com.cloudminds.sg.cdc.connector

import com.cloudminds.sg.cdc.config.PropertiesConfig
import com.ververica.cdc.connectors.mongodb.MongoDBSource
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction

import scala.reflect.ClassTag
import java.util

object MongoConnector {
  def getMongoSource[T: ClassTag](prop:util.HashMap[String, Object]) : SourceFunction[String]={
    val config = PropertiesConfig.getConfig(prop)
    val mongoSource = MongoDBSource.builder[String]()
      .hosts(config.hostname+":"+config.port)
      .username(config.username)
      .password(config.password)
      .databaseList(config.databases.mkString(","))
      .collectionList(config.tables.mkString(","))
      .deserializer(new JsonDebeziumDeserializationSchema(true))
      .build()
    mongoSource
  }
}
