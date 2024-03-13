package com.cloudminds.cdc.service.mysql

import com.cloudminds.cdc.connector.MysqlConnector
import com.cloudminds.cdc.model.source.MysqlSourceModel
import com.cloudminds.cdc.service.SourceFactory
import com.ververica.cdc.connectors.mysql.source.MySqlSource
import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment


class MysqlSourceFactory(model: MysqlSourceModel) extends SourceFactory {

  override def createSource(env: StreamExecutionEnvironment): DataStreamSource[String] = {
    val mysqlSource: MySqlSource[String] = MysqlConnector.getMysqlSource(model)
    val dataStream: DataStreamSource[String] = env.fromSource(mysqlSource, WatermarkStrategy.noWatermarks[String](), "mysql")
    if (null != model.snapshotMode && model.snapshotMode.getValue.equalsIgnoreCase("initial_only")){
      env.setRuntimeMode(RuntimeExecutionMode.BATCH)
    }
    dataStream
  }
}
