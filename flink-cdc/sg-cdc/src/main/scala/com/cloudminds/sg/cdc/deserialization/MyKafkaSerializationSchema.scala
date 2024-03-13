package com.cloudminds.sg.cdc.deserialization

import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema
import org.apache.kafka.clients.producer.ProducerRecord
import java.nio.charset.StandardCharsets
import java.lang

class MyKafkaSerializationSchema(topic:String) extends KafkaSerializationSchema[String]{
  override def serialize(element: String, aLong: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
    //返回一条记录，指定topic为test，分区为0，key为null，value为传入对象转化而成的字节数组
    new ProducerRecord[Array[Byte], Array[Byte]](topic, element.getBytes(StandardCharsets.UTF_8))
  }
}
