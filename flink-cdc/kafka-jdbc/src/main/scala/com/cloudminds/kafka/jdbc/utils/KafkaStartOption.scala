package com.cloudminds.kafka.jdbc.utils

import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaConsumerBase}

import java.util

object KafkaStartOption {
  def getKafkaOption(start: String,kafka: FlinkKafkaConsumer[String],topic:String):FlinkKafkaConsumerBase[String]={
    val startOption: Array[String] = start.toLowerCase.split("-")
    startOption(0) match {
      case "earliest" => kafka.setStartFromEarliest()
      case "latest" => kafka.setStartFromLatest()
      case "timestamp" => kafka.setStartFromTimestamp(startOption(1).toLong)
      case "groupoffsets" => kafka.setStartFromGroupOffsets()
      case "specificoffsets" => kafka.setStartFromSpecificOffsets(getKafkaPartition(startOption(1),topic))
      case _ => kafka.setStartFromGroupOffsets()
    }
  }
  def getKafkaPartition(kafkaInfo: String,topic:String):util.HashMap[KafkaTopicPartition,java.lang.Long]={
    val partitionInfo: Array[String] = kafkaInfo.split(",")
    val partitionId: String = partitionInfo(0)
    val partition = new KafkaTopicPartition(topic, partitionId.toInt)
    val map = new util.HashMap[KafkaTopicPartition, java.lang.Long]()
    map.put(partition,partitionInfo(1).toLong.asInstanceOf[java.lang.Long])
    map
  }
}
