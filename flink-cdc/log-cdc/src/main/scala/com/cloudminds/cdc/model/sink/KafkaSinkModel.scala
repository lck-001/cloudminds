package com.cloudminds.cdc.model.sink

import org.apache.flink.connector.base.DeliveryGuarantee

case class KafkaSinkModel(dbTable: String, transformSql: String, servers: String, transactionTimeoutMs: Long, topic: String, partition: String, deliveryGuarantee: DeliveryGuarantee)
