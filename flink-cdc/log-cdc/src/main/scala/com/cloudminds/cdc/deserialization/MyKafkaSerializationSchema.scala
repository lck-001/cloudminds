package com.cloudminds.cdc.deserialization

import com.alibaba.fastjson.{JSON, JSONObject}
import com.cloudminds.cdc.utils.Placeholder
import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.streaming.connectors.kafka.{KafkaContextAware, KafkaSerializationSchema}
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner
import org.apache.kafka.clients.producer.ProducerRecord

import java.util
import java.lang
import java.nio.charset.StandardCharsets
import scala.reflect.ClassTag


class MyKafkaSerializationSchema(defaultTopic: (String,String),topicMap: util.HashMap[String, (String, String)], partitioner: FlinkKafkaPartitioner[JSONObject]) extends KafkaSerializationSchema[JSONObject] with KafkaContextAware[JSONObject] {
  var partitions: Array[Int] = _
  var targetTopic: String = ""

  override def serialize(json: JSONObject, aLong: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
    var partition: Int = 0
    if (partitioner != null) {
      partition = partitioner.partition(json, null, json.toJSONString.getBytes(StandardCharsets.UTF_8), targetTopic, partitions)
    } else {
      partition = 0
    }
    new ProducerRecord[Array[Byte], Array[Byte]](targetTopic, partition, null, json.toJSONString.getBytes(StandardCharsets.UTF_8))
  }

  override def getTargetTopic(json: JSONObject): String = {
    if ("data".equalsIgnoreCase(defaultTopic._1)){
      // 变量替换方法
      val db: String = json.getString("db")
      val table: String = json.getString("table")
      val placeMap: util.Map[String, Object] = JSON.toJavaObject(json, classOf[util.Map[String, Object]])
      val tp2: (String, String) = topicMap.get(db + "." + table)
      if (tp2 != null && tp2._1 != null) {
        val topic: String = Placeholder.replace(tp2._1, placeMap, "${", "}", false)
        targetTopic = topic
      }else{
        targetTopic = db
      }

      // 正则替换方法
/*
      val db = json.getString("db")
      val table = json.getString("table")
      val tp2 = topicMap.get(db + "." + table)
      if (tp2 != null && tp2._1 != null) {
        var topic = tp2._1
        val pattern = "\\$\\{(\\S*?)\\}".r
        pattern.findAllMatchIn(tp2._1).foreach(g => {
          topic = topic.replaceAll("\\$\\{" + g.group(1) + "\\}", json.get(g.group(1)).asInstanceOf[String])
        })
        targetTopic = topic
      } else {
        targetTopic = db
      }
      */
    }else{
      targetTopic = defaultTopic._2
    }
    targetTopic
  }

  override def setPartitions(partitions: Array[Int]): Unit = {
    super.setPartitions(partitions)
    this.partitions = partitions
  }
}


/*class MyKafkaSerializationSchema(topic:String) extends KafkaSerializationSchema[String]{
  override def serialize(element: String, aLong: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
    //返回一条记录，指定topic为test，分区为0，key为null，value为传入对象转化而成的字节数组
    new ProducerRecord[Array[Byte], Array[Byte]](topic, element.getBytes(StandardCharsets.UTF_8))
  }
}*/


// 以下是泛型实现,意义不大,需要关心泛型的具体数据结构

/*

class MyKafkaSerializationSchema[T : ClassTag](topicsMap : util.HashMap[String,String], com.cloudminds.cdc.partitioner : FlinkKafkaPartitioner[T]) extends KafkaSerializationSchema[T] with KafkaContextAware[T] {
  val writeTimestamp = false
  var partitions: Array[Int] = _
  var targetTopic : String = ""
  override def serialize(t: T, aLong: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
    var partition : Int = 0
    if (com.cloudminds.cdc.partitioner != null) {
      partition = com.cloudminds.cdc.partitioner.partition(t,null.asInstanceOf[Array[Byte]],t.toString.getBytes(StandardCharsets.UTF_8),targetTopic,partitions)
    }else{
      partition = null
    }
    new ProducerRecord[Array[Byte], Array[Byte]](targetTopic,partition,null,t.toString.getBytes(StandardCharsets.UTF_8))
  }

  override def getTargetTopic(t: T): String = {
    /**
     * 获取topic,根据topic获取对应的分区数量
     * 通过数据获取是否有传入当前数据的topic分区,如果有就返回当前分区,否则返回当前的数据库作为分区
      */
     t.getClass.getDeclaredFields
    val json = JSON.parseObject(t.toString)
    val db = json.getString("db")
    val table = json.getString("table")
    val topic = topicsMap.getOrDefault(db + "-" + table, "")
    if (topic.nonEmpty){
      targetTopic = topic
    }else{
      targetTopic = db
    }
    targetTopic
  }

  override def setPartitions(partitions: Array[Int]): Unit = {
    this.partitions = partitions
  }
}

*/



