package com.cloudminds.cdc.service.mongo

import com.cloudminds.cdc.connector.MongoConnector
import com.cloudminds.cdc.model.source.MongoSourceModel
import com.cloudminds.cdc.service.SourceFactory
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.source.SourceFunction

class MongoSourceFactory(model:MongoSourceModel) extends SourceFactory{
  override def createSource(env: StreamExecutionEnvironment): DataStreamSource[String] = {
    val mongoSource: SourceFunction[String] = MongoConnector.getMongoSource(model)
    val dataStream: DataStreamSource[String] = env.addSource(mongoSource)
    dataStream
  }
}
