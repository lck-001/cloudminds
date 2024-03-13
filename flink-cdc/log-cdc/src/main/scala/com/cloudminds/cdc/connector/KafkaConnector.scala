package com.cloudminds.cdc.connector

import com.cloudminds.cdc.deserialization.DeserializationStringSchema
import com.cloudminds.cdc.model.source.KafkaSourceModel
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.kafka.clients.consumer.OffsetResetStrategy

import java.util.Properties

object KafkaConnector {
  def getKafkaConnector(topic: String,properties:Properties):FlinkKafkaConsumer[String]={
    val kafkaConsumer = new FlinkKafkaConsumer[String](topic, new DeserializationStringSchema(), properties)
    kafkaConsumer
  }
  def getKafkaSource(model: KafkaSourceModel):KafkaSource[String]={
    KafkaSource.builder[String]()
      .setGroupId(model.groupId)
      .setStartingOffsets(getOffsetsInitializer(model.startingOffsets))
//      .setProperties(properties)
      .setTopics(model.topic)
      .setValueOnlyDeserializer(new DeserializationStringSchema())
      .setBootstrapServers(model.bootstrapServers)
      .build()
  }
  def getOffsetsInitializer(startingOffsets: String):OffsetsInitializer={
    val startArray: Array[String] = startingOffsets.split("-")
    startArray.head match {
      case "earliest" => OffsetsInitializer.earliest()
      case "latest" => OffsetsInitializer.latest()
      case "timestamp" => OffsetsInitializer.timestamp(startArray.last.toLong)
      case "committedOffsets" => {
        if (null != startArray && startArray.last.equalsIgnoreCase("earliest")){
          OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST)
        }else if (null != startArray && startArray.last.equalsIgnoreCase("latest")){
          OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST)
        }else{
          OffsetsInitializer.committedOffsets()
        }
      }
      case _ => OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST)
    }
  }
}
