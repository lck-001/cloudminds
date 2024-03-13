package com.cloudminds.cdc.service.kafka

import com.cloudminds.cdc.connector.KafkaConnector
import com.cloudminds.cdc.model.source.KafkaSourceModel
import com.cloudminds.cdc.service.SourceFactory
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

class KafkaSourceFactory(model: KafkaSourceModel) extends SourceFactory {
  override def createSource(env: StreamExecutionEnvironment): DataStreamSource[String] = {
    val kafkaSource: KafkaSource[String] = KafkaConnector.getKafkaSource(model)
    val dataStream: DataStreamSource[String] = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks[String](), "kafka")
    dataStream
  }
}
