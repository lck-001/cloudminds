package com.cloudminds.cdc

import com.alibaba.fastjson.JSONObject
import com.cloudminds.cdc.config.{BasePropertiesFactory, PropertiesUtils, SchemaPropertiesFactory}
import com.cloudminds.cdc.deserialization.MyKafkaRecordSerializationSchema
import com.cloudminds.cdc.model.base.BaseModel
import com.cloudminds.cdc.model.sink.SchemaSinkModel
import com.cloudminds.cdc.model.{CommonProp, CommonSource}
import com.cloudminds.cdc.partitioner.CustomerPartitioner
import com.cloudminds.cdc.service.DbFunction
import com.cloudminds.cdc.service.kafka.KafkaDbFunction
import com.cloudminds.cdc.service.mongo.MongoDbFunction
import com.cloudminds.cdc.service.mqtt.MqttDbFunction
import com.cloudminds.cdc.service.mysql.MysqlDbFunction
import com.cloudminds.cdc.service.psql.PsqlDbFunction
import com.cloudminds.cdc.service.sink.{SinkHbase, SinkHdfs, SinkJdbc, SinkKafka}
import com.cloudminds.cdc.transform.{CSFilterFunction, CommonKeySelector, GenRowMapFunction, RepairMapFunction, SchemaCheckProcessFunction, SplitStreamProcessFunction}
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.connector.kafka.sink.KafkaSink
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend
import org.apache.flink.formats.avro.AvroRowDeserializationSchema
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.datastream.{DataStream, SingleOutputStreamOperator}
import org.apache.flink.streaming.api.environment.{CheckpointConfig, StreamExecutionEnvironment}
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.bridge.java.{StreamStatementSet, StreamTableEnvironment}
import org.apache.flink.types.Row
import org.apache.flink.util.OutputTag
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._
import java.util
import java.util.Properties

object LogCdc {
  def main(args: Array[String]): Unit = {
    val logger: Logger = LoggerFactory.getLogger("LogCdc")
    System.setProperty("HADOOP_USER_NAME", "hdfs")
//    var confPath = "/opt/soft/flink-1.13.5/config.yaml"
    var confPath = "/cloudminds/bigdata/flink/config/config.yaml"
    try {
      val params: ParameterTool = ParameterTool.fromArgs(args)
      if (params.has("conf_path")) {
        confPath = params.get("conf_path")
      }
    } catch {
      case e: Exception => {
        logger.error("need config file, please check config ! "+e.printStackTrace())
        throw new Exception("need config file, please check config ! "+e.printStackTrace())
      }
    }
    // 获取base配置
    val baseFactory = new BasePropertiesFactory(confPath)
    val baseModel: BaseModel = baseFactory.getProperties

    // 获取sink配置
    // 获取

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment()
    env.enableCheckpointing(baseModel.checkpointTime)
    env.getCheckpointConfig.setCheckpointingMode(baseModel.checkpointMode)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(baseModel.checkpointPause)
    env.getCheckpointConfig.setCheckpointTimeout(baseModel.checkpointTimeout)
    env.getCheckpointConfig.setExternalizedCheckpointCleanup(baseModel.checkpointCleanup)
    env.setStateBackend(new EmbeddedRocksDBStateBackend)
    // 测试环境
    //    env.getCheckpointConfig.setCheckpointStorage("hdfs://nameservice-ha/tmp/flink/checkpoints/project/")
    // 本地开发环境
    //    env.getCheckpointConfig.setCheckpointStorage("hdfs://cdh-master.cloudminds.com/tmp/flink/checkpoints/project/")
    // 生产环境
    env.getCheckpointConfig.setCheckpointStorage(baseModel.checkpointPath)
    //    env.setStateBackend(new FsStateBackend("file://opt/soft/flink-1.13.5/", true))
    // 设置 checkpoint 的并发度为 1
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(baseModel.maxConcurrentCheckpoint)
//    env.getConfig.registerTypeWithKryoSerializer(Charset.forName("UTF-8").getClass,classOf[CustomerCharsetSerializer])
    env.getConfig.enableForceAvro()

    // 创建table env
    val settings: EnvironmentSettings = EnvironmentSettings
      .newInstance()
      .inStreamingMode()
      //          .useBlinkPlanner()
      .build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)
    val statementSet: StreamStatementSet = tableEnv.createStatementSet()

    val dbFunction:DbFunction = baseModel.sourceType.toLowerCase match {
      case "mysql" => new MysqlDbFunction(confPath, env)
      case "postgre" => new PsqlDbFunction(confPath, env)
      case "kafka" => new KafkaDbFunction(confPath, env)
      case "mongo" => new MongoDbFunction(confPath, env)
      case "mqtt" => new MqttDbFunction(confPath,env)
      case "_" => throw new Exception("unsupported source type")
    }
    dbFunction.createEnv()
    val commonProp: CommonProp = dbFunction.commonProp
    // 获取到的所有数据
    val fullDs: SingleOutputStreamOperator[CommonSource] = dbFunction.ds.map(new RepairMapFunction(commonProp)).uid("repair-info").name("repair info")
    // 过滤掉不符合规则的数据
    val filterDs: SingleOutputStreamOperator[CommonSource] = fullDs.filter(new CSFilterFunction)
    // 按照dbTable分组
    // 检测当前存储的schema信息是否与之前的一致，不一致 输出数据库信息名 表名 变更之前schema字段 变更之后schema字段
    // 定义侧输出 输出当前
    val changeSchemaTag =new OutputTag[JSONObject]("changeSchemaTag",Types.GENERIC(classOf[JSONObject]))
//    val changeSchemaTag =new OutputTag[String]("changeSchemaTag",Types.GENERIC(classOf[String]))
    val commonStream: SingleOutputStreamOperator[CommonSource] = filterDs.keyBy(new CommonKeySelector).process(new SchemaCheckProcessFunction(changeSchemaTag)).uid("schema-check").name("schema check")

    // 定义侧输出
    // 创建tag 准备分流
    val tagMap = new util.HashMap[String, OutputTag[GenericRecord]]()
    for (tag <- commonProp.dataSchema){
      val outputTag = new OutputTag[GenericRecord](tag._1,Types.GENERIC(classOf[GenericRecord]))
      tagMap.put(tag._1,outputTag)
    }
    val mainStream: SingleOutputStreamOperator[CommonSource] = commonStream.process(new SplitStreamProcessFunction(tagMap,commonProp)).uid("split-stream-table").name("split stream table")

    for (tag <- tagMap.keySet()){
      val outputTag: OutputTag[GenericRecord] = tagMap.get(tag)
      val splitStream: DataStream[GenericRecord] = mainStream.getSideOutput(outputTag)
//      splitStream.print(tag)
      // 把流注册成表 先获取对应数据流的schema信息
      val schemaStr: String = commonProp.dataSchema.get(tag)
      // 根据获取到的schema信息注册把对应的流转换成对应的表
      val avroRowDeserializationSchema = new AvroRowDeserializationSchema(schemaStr)
      val rowTypeInfo: RowTypeInfo = avroRowDeserializationSchema.getProducedType.asInstanceOf[RowTypeInfo]
      val rowStream: SingleOutputStreamOperator[Row] = splitStream.map(new GenRowMapFunction).returns(rowTypeInfo)
      val table: Table = tableEnv.fromChangelogStream(rowStream)
      tableEnv.createTemporaryView(tag,table)
      tableEnv.executeSql("DESCRIBE "+tag).print()

      // 写入到hdfs
      val sinkHdfs = new SinkHdfs(confPath, tag, tableEnv)
      if (null != sinkHdfs.hdfsSinkModel && outputTag.getId.equalsIgnoreCase(sinkHdfs.hdfsSinkModel.dbTable)){
        sinkHdfs.transformTable()
        sinkHdfs.executeSink()
      }

      // 写入到kafka
      val sinkKafka = new SinkKafka(confPath, tag, tableEnv)
      if (null != sinkKafka.kafkaSinkModel && outputTag.getId.equalsIgnoreCase(sinkKafka.kafkaSinkModel.dbTable)){
        sinkKafka.transformTable()
        sinkKafka.executeSink()
      }

      // 写入到jdbc
      val sinkJdbc = new SinkJdbc(confPath, tag, tableEnv)
      if (null != sinkJdbc.jdbcSinkModel && outputTag.getId.equalsIgnoreCase(sinkJdbc.jdbcSinkModel.dbTable)){
        sinkJdbc.transformTable()
        sinkJdbc.executeSink()
      }

      // 写入到hbase
      val sinkHbase = new SinkHbase(confPath, tag, tableEnv, statementSet)
      if (null != sinkHbase.hbaseSinkModel && outputTag.getId.equalsIgnoreCase(sinkHbase.hbaseSinkModel.dbTable)){
        sinkHbase.transformTable()
        sinkHbase.executeSink()
      }

    }
    statementSet.attachAsDataStream()


    // 获取变化的schema信息
    val schemaStream: DataStream[JSONObject] = commonStream.getSideOutput(changeSchemaTag)
    val schemaSinkModel: SchemaSinkModel = new SchemaPropertiesFactory(confPath).getProperties
    if (schemaSinkModel.enable) {
      val schemaProp = new Properties()
      schemaProp.put("transaction.timeout.ms", schemaSinkModel.transactionTimeoutMs.toString)
      val kafkaSink: KafkaSink[JSONObject] = KafkaSink.builder[JSONObject]()
        .setBootstrapServers(schemaSinkModel.servers)
        .setKafkaProducerConfig(schemaProp)
        .setTransactionalIdPrefix("schema"+"_"+System.nanoTime())
        .setRecordSerializer(new MyKafkaRecordSerializationSchema("schema",schemaSinkModel.topic, new CustomerPartitioner("")))
        .setDeliverGuarantee(schemaSinkModel.deliveryGuarantee).build()
      schemaStream.sinkTo(kafkaSink).uid("schema-change-event").name("schema change event")
    }
    env.execute(baseModel.jobName)
  }
}
