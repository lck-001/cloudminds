package com.cloudminds.cdc.connector

import com.cloudminds.cdc.model.source.MongoSourceModel
import com.ververica.cdc.connectors.mongodb.MongoDBSource
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction


import scala.reflect.ClassTag

object MongoConnector {
  def getMongoSource[T: ClassTag](mongoModel:MongoSourceModel) : SourceFunction[String]={
    val mongoSource = MongoDBSource.builder[String]()
      .hosts(mongoModel.hostname+":"+mongoModel.port)
      .username(mongoModel.username)
      .password(mongoModel.password)
      .databaseList(mongoModel.databases.mkString(",").replaceAll("`",""))
      .collectionList(mongoModel.dbTables.mkString(",").replaceAll("`",""))
      .deserializer(new JsonDebeziumDeserializationSchema(true))
      .build()
    mongoSource
  }
}
