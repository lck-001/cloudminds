package com.cloudminds.cdc.service.sink

import com.cloudminds.cdc.bucket.CustomerBucketAssigner
import com.cloudminds.cdc.config.HdfsSinkPropertiesFactory
import com.cloudminds.cdc.model.sink.HdfsSinkModel
import com.cloudminds.cdc.schema.CustomParquetWriter
import com.cloudminds.cdc.service.{SinkFactory, SinkOperator, TransformFactory}
import com.cloudminds.cdc.transform.ConvertRowToAvroRecordMapFunction
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter
import org.apache.flink.streaming.api.datastream.{DataStream, SingleOutputStreamOperator}
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy
import org.apache.flink.table.api.{SqlDialect, Table}
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.apache.flink.table.catalog.ResolvedSchema
import org.apache.flink.types.Row

class SinkHdfs(confPath: String, tag: String, tableEnv: StreamTableEnvironment) extends SinkOperator {
  var hdfsSinkModel: HdfsSinkModel = _
  var table: Table = _

/*  getSinkProp()*/

  override def getSinkProp(): Unit = {
    val sinkPropertiesFactory = new HdfsSinkPropertiesFactory(confPath)
    hdfsSinkModel = sinkPropertiesFactory.getProperties.get(tag)
  }

  override def transformTable(): Unit = {
    var transformSql: String = "select * from "+tag
    if (null != hdfsSinkModel && null != hdfsSinkModel.transformSql && hdfsSinkModel.transformSql.nonEmpty){
      val sql_arr: Array[String] = hdfsSinkModel.transformSql.toLowerCase().split("\\s(?i)from\\s")
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

    genericStream.addSink(getSink(schema.toString)).uid(tag+"-sink-to-hdfs-parquet").name("sink to hdfs parquet")
//    val schema: ResolvedSchema = table.getResolvedSchema
  }

  private def getSink(schemaStr:String):StreamingFileSink[GenericRecord] = {
    val sinkClo: StreamingFileSink[GenericRecord] = StreamingFileSink
      .forBulkFormat(
        new Path(hdfsSinkModel.hdfsPath),
        CustomParquetWriter.forStringToGenericRecord(schemaStr)
      ).withBucketCheckInterval(1000)
      .withRollingPolicy(OnCheckpointRollingPolicy.build())
      .withBucketAssigner(new CustomerBucketAssigner(hdfsSinkModel.partitions))
      .build()
    sinkClo
  }
}
