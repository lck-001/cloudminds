package com.cloudminds.cdc.config

import com.cloudminds.cdc.model.sink.SchemaSinkModel
import org.apache.flink.connector.base.DeliveryGuarantee

import java.util

class SchemaPropertiesFactory (confPath: String) extends PropertiesFactory[SchemaSinkModel]{
  override def getProperties: SchemaSinkModel = {
    val propMap: util.HashMap[String, Object] = PropertiesUtils.getProperties(confPath).get("sink").asInstanceOf[util.HashMap[String, Object]]
    val sinkSchemaMap: util.HashMap[String, Object] = propMap.get("sinkSchema").asInstanceOf[util.HashMap[String, Object]]
    val enable: Boolean = Option(sinkSchemaMap.get("enable")).getOrElse("true").toString.toBoolean
    val servers: String = sinkSchemaMap.get("bootstrapServers").toString
    val transactionTimeoutMs: Long = Option(sinkSchemaMap.get("transactionTimeoutMs")).getOrElse("30000").toString.toLong
    val topic: String = Option(sinkSchemaMap.get("topic")).getOrElse("").toString
    val ack: String = Option(sinkSchemaMap.get("transactionAck")).getOrElse("at_least_once").toString.toLowerCase
    val deliveryGuarantee: DeliveryGuarantee = ack match {
      case "none" => DeliveryGuarantee.NONE
      case "at_least_once" => DeliveryGuarantee.AT_LEAST_ONCE
      case "exactly_once" => DeliveryGuarantee.EXACTLY_ONCE
    }
    SchemaSinkModel(enable = enable, servers = servers, transactionTimeoutMs = transactionTimeoutMs, topic = topic, deliveryGuarantee = deliveryGuarantee)
  }
}
