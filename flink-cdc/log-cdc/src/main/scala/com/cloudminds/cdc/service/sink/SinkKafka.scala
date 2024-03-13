package com.cloudminds.cdc.service.sink

import com.alibaba.fastjson.JSONObject
import com.cloudminds.cdc.config.KafkaSinkPropertiesFactory
import com.cloudminds.cdc.deserialization.MyKafkaRecordSerializationSchema
import com.cloudminds.cdc.model.sink.KafkaSinkModel
import com.cloudminds.cdc.partitioner.CustomerPartitioner
import com.cloudminds.cdc.service.SinkOperator
import com.cloudminds.cdc.transform.{ConvertRowToAvroRecordMapFunction, GenJsonMapFunction}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.kafka.sink.KafkaSink
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter
import org.apache.flink.streaming.api.datastream.{DataStream, SingleOutputStreamOperator}
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.apache.flink.types.Row

import java.util.Properties

class SinkKafka(confPath: String, tag: String, tableEnv: StreamTableEnvironment) extends SinkOperator{
  var kafkaSinkModel:KafkaSinkModel = _
  var table: Table = _

  override def getSinkProp(): Unit = {
    val propertiesFactory = new KafkaSinkPropertiesFactory(confPath)
    kafkaSinkModel = propertiesFactory.getProperties.get(tag)
  }

  override def transformTable(): Unit = {
    var transformSql: String = "select * from "+tag
    if (null != kafkaSinkModel && null != kafkaSinkModel.transformSql && kafkaSinkModel.transformSql.nonEmpty){
      val sql_arr: Array[String] = kafkaSinkModel.transformSql.toLowerCase().split("\\s(?i)from\\s")
      transformSql = sql_arr(0).replaceAll("\\.", "_P_").replaceAll("\\$","_D_")+" from "+sql_arr(1)
    }
    // 执行sql转换操作
    //    tableEnv.getConfig.setSqlDialect(SqlDialect.HIVE)
    table = tableEnv.sqlQuery(transformSql)
  }

  override def executeSink(): Unit = {
    // table 转流
    val dataSteam: DataStream[Row] = tableEnv.toChangelogStream(table)
//    dataSteam.print(" transform "+tag)
    val schema: Schema = AvroSchemaConverter.convertToSchema(table.getResolvedSchema.toSourceRowDataType.getLogicalType)
    val genericStream: SingleOutputStreamOperator[GenericRecord] = dataSteam.map(new ConvertRowToAvroRecordMapFunction(schema.toString))
    val jsonStream: SingleOutputStreamOperator[JSONObject] = genericStream.map(new GenJsonMapFunction)
    jsonStream.sinkTo(getSink).uid(tag+"-sink-to-kafka-string").name(tag+" sink to kafka string")
  }

  def getSink:KafkaSink[JSONObject]={
    val kafkaProp = new Properties()
    kafkaProp.put("flink.partition-discovery.interval-millis",(10 * 1000).toString)
    kafkaProp.put("transaction.timeout.ms",kafkaSinkModel.transactionTimeoutMs.toString)
    val kafkaSink: KafkaSink[JSONObject] = KafkaSink.builder[JSONObject]()
      .setBootstrapServers(kafkaSinkModel.servers)
      .setKafkaProducerConfig(kafkaProp)
      .setTransactionalIdPrefix(tag)
      .setRecordSerializer(new MyKafkaRecordSerializationSchema("data",kafkaSinkModel.topic, new CustomerPartitioner(kafkaSinkModel.partition)))
      .setDeliverGuarantee(kafkaSinkModel.deliveryGuarantee).build()
    kafkaSink
  }

}
