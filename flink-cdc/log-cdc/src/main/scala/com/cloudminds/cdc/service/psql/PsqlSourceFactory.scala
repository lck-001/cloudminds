package com.cloudminds.cdc.service.psql

import com.cloudminds.cdc.connector.PsqlConnector
import com.cloudminds.cdc.model.source.PsqlSourceModel
import com.cloudminds.cdc.service.SourceFactory
import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.source.SourceFunction

class PsqlSourceFactory(model: PsqlSourceModel) extends SourceFactory {
  override def createSource(env: StreamExecutionEnvironment): DataStreamSource[String] = {
//    val model: PsqlModel = psqlFactory.getProperties
    val psqlSource: SourceFunction[String] = PsqlConnector.getPostgresSource(model)
    val dataStream: DataStreamSource[String] = env.addSource(psqlSource)
    if (null != model.snapshotMode && model.snapshotMode.getValue.equalsIgnoreCase("initial_only")){
      env.setRuntimeMode(RuntimeExecutionMode.BATCH)
    }
    dataStream
  }
}
