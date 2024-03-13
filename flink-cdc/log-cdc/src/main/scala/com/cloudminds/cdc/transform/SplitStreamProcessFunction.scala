package com.cloudminds.cdc.transform

import com.cloudminds.cdc.model.{CommonProp, CommonSource}
import com.cloudminds.cdc.service.ExtractFactory
import com.cloudminds.cdc.service.kafka.KafkaExtractFactory
import com.cloudminds.cdc.service.mongo.MongoExtractFactory
import com.cloudminds.cdc.service.mqtt.MqttExtractFactory
import com.cloudminds.cdc.service.mysql.MysqlExtractFactory
import com.cloudminds.cdc.service.psql.PsqlExtractFactory
import org.apache.avro.generic.GenericRecord
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.{Collector, OutputTag}

import scala.collection.JavaConversions._
import java.util



class SplitStreamProcessFunction(tag : util.HashMap[String, OutputTag[GenericRecord]],commonProp:CommonProp) extends ProcessFunction[CommonSource,CommonSource]{
  val tagMap : util.HashMap[String, OutputTag[GenericRecord]] = new util.HashMap[String,OutputTag[GenericRecord]]()
  // schema tableName,schemaStr
  val schemaMap: util.HashMap[String,String] = new util.HashMap[String,String]()
  var extractFactory: ExtractFactory = _
  override def open(parameters: Configuration): Unit = {
    // 主要是处理sink中如果出现了关键字作为表名,需要使用'`'来包裹关键字
    for (kv <- tag){
      val key: String = kv._1.replaceAll("`", "")
      tagMap.put(key,kv._2)
    }
    for (kv <- commonProp.dataSchema){
      val key: String = kv._1.replaceAll("`", "")
      schemaMap.put(key,kv._2)
    }
    extractFactory = commonProp.sourceType.toLowerCase match {
      case "mysql" => new MysqlExtractFactory(schemaMap)
      case "postgre" => new PsqlExtractFactory(schemaMap)
      case "kafka" => new KafkaExtractFactory(schemaMap)
      case "mongo" => new MongoExtractFactory(schemaMap)
      case "mqtt" => new MqttExtractFactory(schemaMap)
    }
  }

  override def processElement(common: CommonSource, context: ProcessFunction[CommonSource, CommonSource]#Context, collector: Collector[CommonSource]): Unit = {
    collector.collect(common)
    // 获取schema 封装GenericRecord数据类型
    val dbTable: String = common.dbTable
    val outputTag: OutputTag[GenericRecord] = tagMap.get(dbTable)
    val schemaStr: String = schemaMap.get(dbTable)
    if (null != outputTag && null != schemaStr && schemaStr.nonEmpty){
      val record: GenericRecord = extractFactory.getGenericRecord(common)
      context.output(outputTag,record)
    }else{
      throw new Exception("no schema defined or no tag defined !!!!")
    }
  }
}