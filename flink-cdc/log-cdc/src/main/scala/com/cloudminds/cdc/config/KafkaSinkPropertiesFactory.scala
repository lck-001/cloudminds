package com.cloudminds.cdc.config

import com.cloudminds.cdc.model.sink.KafkaSinkModel
import org.apache.flink.connector.base.DeliveryGuarantee

import scala.collection.JavaConversions._
import java.util

class KafkaSinkPropertiesFactory(confPath: String) extends PropertiesFactory[util.HashMap[String, KafkaSinkModel]] {
  override def getProperties: util.HashMap[String, KafkaSinkModel] = {
    val propMap: util.HashMap[String, Object] = PropertiesUtils.getProperties(confPath).get("sink").asInstanceOf[util.HashMap[String, Object]]
    val sinkKafkaList: util.ArrayList[util.HashMap[String, Object]] = propMap.get("sinkKafka").asInstanceOf[util.ArrayList[util.HashMap[String, Object]]]
    val kafkaSinkMap = new util.HashMap[String, KafkaSinkModel]
    if (null != sinkKafkaList && sinkKafkaList.nonEmpty) {
      for (sinkKafka <- sinkKafkaList ) {
        val dbTable: String = sinkKafka.get("dbTable").toString
        val servers: String = sinkKafka.get("bootstrapServers").toString
        val transactionTimeoutMs: Long = Option(sinkKafka.get("transactionTimeoutMs")).getOrElse("30000").toString.toLong
        val transformSql: String = Option(sinkKafka.get("transformSql")).getOrElse("").toString
        val topic: String = Option(sinkKafka.get("topic")).getOrElse("").toString
        val partition: String = Option(sinkKafka.get("partition")).getOrElse("").toString
        val ack: String = Option(sinkKafka.get("transactionAck")).getOrElse("at_least_once").toString.toLowerCase
        val deliveryGuarantee: DeliveryGuarantee = ack match {
          case "none" => DeliveryGuarantee.NONE
          case "at_least_once" => DeliveryGuarantee.AT_LEAST_ONCE
          case "exactly_once" => DeliveryGuarantee.EXACTLY_ONCE
        }
        kafkaSinkMap.put(dbTable, KafkaSinkModel(dbTable = dbTable, transformSql = transformSql, servers = servers, transactionTimeoutMs = transactionTimeoutMs, topic = topic, partition = partition, deliveryGuarantee = deliveryGuarantee))
      }
    }
    kafkaSinkMap
  }
}
