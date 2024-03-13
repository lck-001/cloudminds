package com.cloudminds.cdc.partitioner

import com.alibaba.fastjson.JSONObject
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner

import java.util
import scala.reflect.ClassTag

class CustomerPartitioner(partition: String) extends FlinkKafkaPartitioner[JSONObject] {
  var index: Int = 0

  override def partition(json: JSONObject, key: Array[Byte], value: Array[Byte], s: String, partitions: Array[Int]): Int = {
    if (null != partition && partition.nonEmpty) {
      // 如果指定了key使用key分区,否则轮询写入分区
      Math.abs(new String(partition).hashCode() % partitions.length)
    } else {
      if (index <= 10000) {
        index += 1
        index & (partitions.length - 1)
      } else {
        index = 0
        index & (partitions.length - 1)
      }
    }
  }
}

// 以下是泛型实现意义不大
/*

class CustomerPartitioner[T : ClassTag] extends FlinkKafkaPartitioner {
  override def partition(t: Nothing, key: Array[Byte], value: Array[Byte], s: String, partitions: Array[Int]): Int = {
    Math.abs(new String(key).hashCode() % partitions.length)
  }
}
*/

/*
override def partition(t: JSONObject, key: Array[Byte], value : Array[Byte], s: String, partitions: Array[Int]): Int = {
  Math.abs(new String(key).hashCode() % partitions.length)
  }
*/
