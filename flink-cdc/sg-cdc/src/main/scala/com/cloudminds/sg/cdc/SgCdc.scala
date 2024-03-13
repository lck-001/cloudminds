package com.cloudminds.sg.cdc

import com.cloudminds.sg.cdc.config.PropertiesUtils
import com.cloudminds.sg.cdc.connector.{MongoConnector, MysqlConnector, PsqlConnector}
import com.cloudminds.sg.cdc.deserialization.{DeserializationStringSchema, MyKafkaSerializationSchema}
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink, KafkaSinkBuilder}
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.{CheckpointConfig, StreamExecutionEnvironment}

import java.util
import java.util.Properties

object SgCdc {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    var conf_path = "/opt/soft/flink-1.13.5/config.yaml"
    try {
      val params = ParameterTool.fromArgs(args)
      if (params.has("conf_path")) {
        conf_path = params.get("conf_path")
      }
    } catch {
      case e: Exception => {
        throw new Exception("need config file, please check config ! "+e.printStackTrace())
      }
    }

    // 配置文件参数解析
    val prop = PropertiesUtils.getPropertiesMap(conf_path)

    val checkpointTime = prop.getOrDefault("checkpointTime","60000").toString.toLong
    val checkpointPause = prop.getOrDefault("checkpointPause","5000").toString.toLong
    val checkpointTimeout = prop.getOrDefault("checkpointTimeout","120000").toString.toLong
    val checkpointPath = prop.getOrDefault("checkpointPath","hdfs://cdh-master.cloudminds.com/tmp/flink/checkpoints/project/").asInstanceOf[String]
    val maxConcurrentCheckpoint = prop.getOrDefault("maxConcurrentCheckpoint","1").toString.toInt
    val kafkaSchemaProp = new Properties()
    kafkaSchemaProp.put("flink.partition-discovery.interval-millis",(10 * 1000).toString)
    var topic = ""
    val kafkaServers = prop.getOrDefault("kafka",new util.ArrayList[util.HashMap[String,String]]()).asInstanceOf[util.ArrayList[util.HashMap[String,String]]]
    for (kafkaServer <-kafkaServers.toArray if !kafkaServers.isEmpty){
      val kafka = kafkaServer.asInstanceOf[util.HashMap[String, String]]
      if (kafka.containsKey("type") && kafka.getOrDefault("type","").equalsIgnoreCase("data")){
        topic = kafka.getOrDefault("topic","")
        kafkaSchemaProp.put("bootstrap.servers",kafka.getOrDefault("servers",""))
        kafkaSchemaProp.put("transaction.timeout.ms",kafka.getOrDefault("transaction.timeout.ms","30000"))
      }
    }
    val env = StreamExecutionEnvironment.getExecutionEnvironment()

    //    env.setParallelism(4)
    //启动checkpoint
    env.enableCheckpointing(checkpointTime)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(checkpointPause)
    env.getCheckpointConfig.setCheckpointTimeout(checkpointTimeout)
    env.getCheckpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    env.setStateBackend(new EmbeddedRocksDBStateBackend)
    // 测试环境
    //    env.getCheckpointConfig.setCheckpointStorage("hdfs://nameservice-ha/tmp/flink/checkpoints/project/")
    // 本地开发环境
    //    env.getCheckpointConfig.setCheckpointStorage("hdfs://cdh-master.cloudminds.com/tmp/flink/checkpoints/project/")
    // 生产环境
    env.getCheckpointConfig.setCheckpointStorage(checkpointPath)

    // 设置 checkpoint 的并发度为 1
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(maxConcurrentCheckpoint)

    env.getCheckpointConfig.enableUnalignedCheckpoints()

    var dataStream:DataStreamSource[String] = null
    val sourceType = prop.get("sourceType").asInstanceOf[String]
    if ("mysql".equalsIgnoreCase(sourceType)){
      val mysqlSource = MysqlConnector.getMysqlSource(prop)
      dataStream = env.fromSource(mysqlSource, WatermarkStrategy.noWatermarks[String](), "mysql")
    }else if("postgres".equalsIgnoreCase(sourceType)){
      val psqlSource = PsqlConnector.getPostgresSource(prop)
      dataStream = env.addSource(psqlSource)
    }else if("mongodb".equalsIgnoreCase(sourceType)){
      val mongoSource = MongoConnector.getMongoSource(prop)
      dataStream = env.addSource(mongoSource)
    }

    val changeSchemaSink: KafkaSink[String] = KafkaSink.builder[String]()
      .setBootstrapServers(kafkaSchemaProp.getProperty("bootstrap.servers"))
      .setKafkaProducerConfig(kafkaSchemaProp)
      .setRecordSerializer(KafkaRecordSerializationSchema.builder[String]()
        .setTopic(topic)
        .setKeySerializationSchema(new SimpleStringSchema())
        .setValueSerializationSchema(new SimpleStringSchema())
        .build())
      .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE).build()

//    dataStream.addSink(changeSchemaSink).uid("sink to sg kafka").name("sink to sg kafka")
    dataStream.sinkTo(changeSchemaSink).uid("sink to sg kafka").name("sink to sg kafka")
    env.execute("sg-cdc")
  }
}
