package com.cloudminds.kafka.jdbc

import com.alibaba.fastjson.JSONObject
import com.cloudminds.kafka.jdbc.config.PropertiesUtils
import com.cloudminds.kafka.jdbc.deserialization.DeserializationStringSchema
import com.cloudminds.kafka.jdbc.jdbc.{ConcatSql, ExcludeColumns, JdbcSinkUtil}
import com.cloudminds.kafka.jdbc.model.JdbcSinkProp
import com.cloudminds.kafka.jdbc.schema.{DataTypeMap, RowTypeInfoMap}
import com.cloudminds.kafka.jdbc.sink.JdbcSinkFunction
import com.cloudminds.kafka.jdbc.transform.{ColumnsMapFunction, GenRowMapFunction, SplitStreamProcessFunction, TableFilterFunction}
import com.cloudminds.kafka.jdbc.utils.{KafkaStartOption, SqlParserHandler}
import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend
import org.apache.flink.streaming.api.{CheckpointingMode, datastream}
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSource, SingleOutputStreamOperator}
import org.apache.flink.streaming.api.environment.{CheckpointConfig, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.apache.flink.table.types.DataType
import org.apache.flink.types.Row
import org.apache.flink.util.OutputTag
import org.apache.flink.table.api.{Schema => FlinkSchema}

import org.apache.flink.api.scala._
import scala.collection.JavaConversions._
import java.util
import java.util.Properties
import scala.collection.mutable.ArrayBuffer

object KafkaJdbc {
  def main(args: Array[String]): Unit = {
    var conf_path = "/opt/soft/flink-1.13.5/kafka-jdbc.yaml"
    var kafkaStartMode = "groupoffsets"
    try {
      val params: ParameterTool = ParameterTool.fromArgs(args)
      if (params.has("conf_path")) {
        conf_path = params.get("conf_path")
      }
      if (params.has("kafkaStartMode")){
        kafkaStartMode = params.get("kafkaStartMode")
      }
    } catch {
      case e: Exception => {
        throw new Exception("need config file, please check config ! "+e.printStackTrace())
      }
    }

    // 配置文件参数解析
    var prop:java.util.HashMap[String,Object] = null
    if ("hdfs".equalsIgnoreCase(conf_path.split("://").head)){
      prop = PropertiesUtils.getHdfsPropertiesMap(conf_path)
    }else{
      prop = PropertiesUtils.getPropertiesMap(conf_path)
    }
    val dataEnv: String = Option(prop.get("dataEnv")).getOrElse("bj-prod-232").asInstanceOf[String]
    val jobName: String = Option(prop.get("jobName")).getOrElse("kafka-jdbc").asInstanceOf[String]
    val checkpointTime: Long = prop.getOrDefault("checkpointTime","60000").toString.toLong
    val checkpointPause: Long = prop.getOrDefault("checkpointPause","5000").toString.toLong
    val checkpointTimeout: Long = prop.getOrDefault("checkpointTimeout","120000").toString.toLong
    val checkpointPath: String = Option(prop.get("checkpointPath")).getOrElse("hdfs://cdh-master.cloudminds.com/tmp/flink/checkpoints/project/").asInstanceOf[String]
    val maxConcurrentCheckpoint: Int = Option(prop.get("maxConcurrentCheckpoint")).getOrElse(1).asInstanceOf[Int]
    // 获取同步的表数据
    val dataList = prop.get("data").asInstanceOf[util.ArrayList[util.HashMap[String,Object]]]
    val tables = new util.HashMap[String, util.HashMap[String, String]]()
    for (data <- dataList.toArray if !dataList.isEmpty){
      val dataTableObj: util.HashMap[String, Object] = data.asInstanceOf[util.HashMap[String, Object]]
      val dataTable: String = Option(dataTableObj.get("dataTable")).getOrElse("").toString
      val schemaMap: util.HashMap[String, String] = Option(dataTableObj.get("schema")).getOrElse(new util.HashMap[String, String]()).asInstanceOf[util.HashMap[String, String]]
      schemaMap.put("db", "string")
      schemaMap.put("table", "string")
      schemaMap.put("bigdata_method","string");
//      schemaMap.put("event_time","long");
      schemaMap.put("k8s_env_name","string");
      tables.put(dataTable,schemaMap)
    }
    // 获取读取的kafka配置
    val kafkaProp: util.HashMap[String, String] = prop.get("kafka").asInstanceOf[util.HashMap[String, String]]
    val topicsStr: String = Option(kafkaProp.get("topics")).getOrElse("")
    val groupId: String = Option(kafkaProp.get("groupId")).getOrElse("")
    val servers: String = Option(kafkaProp.get("servers")).getOrElse("")
    val tts: String = Option(kafkaProp.get("transaction.timeout.ms")).getOrElse(30000).toString
    val properties = new Properties
    properties.put("flink.partition-discovery.interval-millis",(10 * 1000).toString)
    properties.put("group.id",groupId)
    properties.put("bootstrap.servers",servers)
    properties.put("transaction.timeout.ms",tts)
    // jdbc table
    val jdbcTables: util.ArrayList[util.HashMap[String, String]] = prop.getOrDefault("jdbcTable", new util.ArrayList[util.HashMap[String, String]]()).asInstanceOf[util.ArrayList[util.HashMap[String, String]]]
    val jdbcTable = new util.HashMap[String, util.HashMap[String, Object]]()
    for (jt <- jdbcTables.toArray if !jdbcTables.isEmpty){
      val jdbcMap = new util.HashMap[String,Object]()
      val jtMap: util.HashMap[String, String] = jt.asInstanceOf[util.HashMap[String, String]]
      val sinkDatabase: String = Option(jtMap.get("sinkDatabase")).getOrElse("")
      val sinkTable: String = Option(jtMap.get("sinkTable")).getOrElse("")
      val sql: String = Option(jtMap.get("sql")).getOrElse("")
      val columns: util.List[String] = SqlParserHandler.getSelectColumns(sql,tables)
      val tablesName: util.List[String] = SqlParserHandler.getSelectTables(sql)
      jdbcMap.put("sinkDatabase",sinkDatabase)
      jdbcMap.put("sinkTable",sinkTable)
      jdbcMap.put("sql",sql)
      jdbcMap.put("columns",columns)
      jdbcTable.put(tablesName.head,jdbcMap)
    }
    // 获取jdbc配置
    val jdbcProp: util.HashMap[String, String] = prop.getOrDefault("jdbc",new util.HashMap[String, String]).asInstanceOf[util.HashMap[String, String]]
    val jdbcDriver: String = Option(jdbcProp.get("jdbcDriver")).getOrElse("")
    val jdbcUrl: String = Option(jdbcProp.get("url")).getOrElse("")
    val jdbcUsername: String = Option(jdbcProp.get("username")).getOrElse("")
    val jdbcPassword: String = Option(jdbcProp.get("password")).getOrElse("")
    val batchSize: Int = Option(jdbcProp.get("batchSize")).getOrElse(100).asInstanceOf[Int]
    val batchInterval: Int = Option(jdbcProp.get("batchInterval")).getOrElse(60000).asInstanceOf[Int]
    val maxRetries: Int = Option(jdbcProp.get("maxRetries")).getOrElse(3).asInstanceOf[Int]
    val jdbcSinkProp: JdbcSinkProp = new JdbcSinkProp.Builder().setJdbcDriver(jdbcDriver).setJdbcUrl(jdbcUrl).setUsername(jdbcUsername).setPassword(jdbcPassword).setBatchSize(batchSize).setBatchInterval(batchInterval).setMaxRetries(maxRetries).build()


    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment()
    env.setParallelism(1)
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
    //    env.setStateBackend(new FsStateBackend("file://opt/soft/flink-1.13.5/", true))
    // 设置 checkpoint 的并发度为 1
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(maxConcurrentCheckpoint)
    val settings: EnvironmentSettings = EnvironmentSettings
      .newInstance()
      .inStreamingMode()
      .useBlinkPlanner()
      .build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)
    // 把schema配置设置成全局变量
    val conf = new Configuration()
    conf.setString("k8s_env_name",dataEnv)
    conf.setString("jdbcDriver",jdbcDriver)
    conf.setString("jdbcUrl",jdbcUrl)
    conf.setString("jdbcUsername",jdbcUsername)
    conf.setString("jdbcPassword",jdbcPassword)
    env.getConfig.setGlobalJobParameters(conf)

    val topics: ArrayBuffer[String] = topicsStr.split(",").toBuffer.asInstanceOf[ArrayBuffer[String]]

    val javaArr = new ProcessBuilder(topics)
    val kafkaSource = new FlinkKafkaConsumer[String](javaArr.command(), new DeserializationStringSchema(), properties)
    KafkaStartOption.getKafkaOption(kafkaStartMode,kafkaSource,topics(0))
//    kafkaSource.setStartFromEarliest()
    val kafkaStream: DataStreamSource[String] = env.addSource(kafkaSource)
    // 过滤出想要的数据
    val dataStream: SingleOutputStreamOperator[String] = kafkaStream.filter(new TableFilterFunction(tables)).uid("table-filter").name("table filter")
//    dataStream.print("data stream ----> ")
    // 过滤出想要的字段
    val jsonStream: SingleOutputStreamOperator[JSONObject] = dataStream.map(new ColumnsMapFunction(tables)).uid("column-filter-map").name("column filter map")
//    jsonStream.print("json stream ----> ")
    // 根据表创建output tag
    val tagMap = new util.HashMap[String, OutputTag[JSONObject]]()
    tables.foreach{
      case (key,value) =>{
        val outputTag = new OutputTag[JSONObject](key,Types.GENERIC(classOf[JSONObject]))
        tagMap.put(key,outputTag)
      }
    }
    // jsonStream 分流
    val mainStream: SingleOutputStreamOperator[JSONObject] = jsonStream.process(new SplitStreamProcessFunction(tagMap)).uid("split-stream").name("split stream")
    for (tag <- tagMap.keySet){
      val outputTag: OutputTag[JSONObject] = tagMap.get(tag)
      val splitStream: DataStream[JSONObject] = mainStream.getSideOutput(outputTag)
      // 获取表的schema
      val columnsSchema: util.HashMap[String, String] = tables.get(tag)
      val fieldsName = new ArrayBuffer[String]()
      val fieldsType = new util.ArrayList[DataType]()
      val fieldsRowType = new ArrayBuffer[TypeInformation[_]]()
      for (column <- columnsSchema){
        if (!ExcludeColumns.createMap().containsKey(column._1)){
          fieldsName.add(column._1)
          fieldsType.add(DataTypeMap.getDataType(column._2))
          fieldsRowType.add(RowTypeInfoMap.getRowType(column._2))
        }
      }

      val flinkSchema: FlinkSchema = FlinkSchema.newBuilder().fromFields(fieldsName, fieldsType).build()
      // splitStream 转换为row
      val rowStream: SingleOutputStreamOperator[Row] = splitStream.map(new GenRowMapFunction(tables)).returns(new RowTypeInfo(fieldsRowType.toArray,fieldsName.toArray)).uid(tag+"gen-json-to-row").name("gen json to row")
//      rowStream.print(tag.getKey)
      val table: Table = tableEnv.fromDataStream(rowStream, flinkSchema)
      tableEnv.createTemporaryView(tag,table)
      tableEnv.executeSql("DESCRIBE "+tag).print()
      var transformSql = ""
      if (jdbcTable.get(tag).getOrDefault("sql","").toString.isEmpty){
        transformSql = "select * from "+tag
      }else{
        transformSql = jdbcTable.get(tag).getOrDefault("sql","").toString
      }
      println("transformsSql-------->"+transformSql)
      val table2: Table = tableEnv.sqlQuery(transformSql)
      val streamRow: datastream.DataStream[Row] = tableEnv.toChangelogStream(table2)
//      streamRow.print(tag+"-----")
//      streamRow.addSink(new JdbcSinkFunction)
      // 拼接insert sql
      val insertSql: String = ConcatSql.getSql(jdbcUrl,jdbcTable.get(tag))
      val jdbcSink: SinkFunction[Row] = (new JdbcSinkUtil).getSink(insertSql, jdbcSinkProp)
      streamRow.addSink(jdbcSink).uid(tag+"sink-to-jdbc-table").name("sink to jdbc table")
    }


    env.execute(jobName)
  }
}
