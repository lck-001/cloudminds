package com.cloudminds.cdc.transform

import com.cloudminds.cdc.model.{CommonProp, CommonSource}
import com.cloudminds.cdc.service.GenCommonSourceFactory
import com.cloudminds.cdc.service.kafka.KafkaGenCommonSourceFactory
import com.cloudminds.cdc.service.mongo.MongoGenCommonSourceFactory
import com.cloudminds.cdc.service.mqtt.MqttGenCommonSourceFactory
import com.cloudminds.cdc.service.mysql.MysqlGenCommonSourceFactory
import com.cloudminds.cdc.service.psql.PsqlGenCommonSourceFactory
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration

import scala.reflect.ClassTag


class RepairMapFunction(commonProp: CommonProp) extends RichMapFunction[String,CommonSource]{
  var transformFactory: GenCommonSourceFactory = _
  override def open(parameters: Configuration): Unit = {
    transformFactory = commonProp.sourceType match {
      case "mysql" => new MysqlGenCommonSourceFactory(commonProp)
      case "psql" => new PsqlGenCommonSourceFactory(commonProp)
      case "kafka" => new KafkaGenCommonSourceFactory(commonProp)
      case "mongo" => new MongoGenCommonSourceFactory(commonProp)
      case "mqtt" => new MqttGenCommonSourceFactory(commonProp)
      case _ => throw new Exception("unsupported source")
    }
  }
  override def map(value: String): CommonSource = {
    val source: CommonSource = transformFactory.getCommonSource(value)
    source
  }
}
