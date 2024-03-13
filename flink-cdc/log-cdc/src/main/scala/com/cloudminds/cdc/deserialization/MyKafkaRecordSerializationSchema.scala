package com.cloudminds.cdc.deserialization

import com.alibaba.fastjson.{JSON, JSONObject}
import com.cloudminds.cdc.utils.Placeholder
import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema.KafkaSinkContext
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner
import org.apache.kafka.clients.producer.ProducerRecord

import java.nio.charset.StandardCharsets
import java.{lang, util}

class MyKafkaRecordSerializationSchema(streamType: String, topicRule: String, partitioner: FlinkKafkaPartitioner[JSONObject]) extends KafkaRecordSerializationSchema[JSONObject] {

  override def open(context: SerializationSchema.InitializationContext, sinkContext: KafkaSinkContext): Unit = {
    super.open(context, sinkContext)
    if (partitioner != null) {
      partitioner.open(sinkContext.getParallelInstanceId, sinkContext.getNumberOfParallelInstances)
    }
  }

  override def serialize(json: JSONObject, context: KafkaRecordSerializationSchema.KafkaSinkContext, timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
    val topic: String = getTargetTopic(json)
    val partitions: Array[Int] = context.getPartitionsForTopic(topic)

    var partition: Int = 0
    if (partitioner != null) {
      partition = partitioner.partition(json, null, json.toJSONString.getBytes(StandardCharsets.UTF_8), topic, partitions)
    } else {
      partition = 0
    }
    new ProducerRecord[Array[Byte], Array[Byte]](topic, partition, null, json.toJSONString.getBytes(StandardCharsets.UTF_8))
  }

  def getTargetTopic(json: JSONObject): String = {
    //默认写入 data topic
    var targetTopic: String = streamType
    if ("data".equalsIgnoreCase(streamType)){
      // 变量替换方法
      val placeMap: util.Map[String, Object] = JSON.toJavaObject(json, classOf[util.Map[String, Object]])
      if (null != topicRule && topicRule.nonEmpty) {
        val topic: String = Placeholder.replace(topicRule, placeMap, "${", "}", false)
        targetTopic = topic
      }
    }
    targetTopic
  }

}
