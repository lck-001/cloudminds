package com.cloudminds.cdc.model.sink

import org.apache.flink.connector.base.DeliveryGuarantee

case class SchemaSinkModel(enable:Boolean, servers: String, transactionTimeoutMs: Long, topic: String, deliveryGuarantee: DeliveryGuarantee)
