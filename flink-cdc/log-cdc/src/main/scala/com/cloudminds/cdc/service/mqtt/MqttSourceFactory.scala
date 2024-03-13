package com.cloudminds.cdc.service.mqtt

import com.cloudminds.cdc.connector.MqttConnector
import com.cloudminds.cdc.model.source.MqttSourceModel
import com.cloudminds.cdc.service.SourceFactory
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

class MqttSourceFactory(model:MqttSourceModel) extends SourceFactory{
  override def createSource(env: StreamExecutionEnvironment): DataStreamSource[String] = {
    val mqttConnector = new MqttConnector(model)
    val mqttSource: DataStreamSource[String] = env.addSource(mqttConnector)
    mqttSource
  }
}
