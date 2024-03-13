package com.cloudminds.cdc.service.sink

import com.cloudminds.cdc.config.JdbcSinkPropertiesFactory
import com.cloudminds.cdc.jdbc.{ConcatSql, JdbcSinkUtil}
import com.cloudminds.cdc.model.sink.JdbcSinkModel
import com.cloudminds.cdc.service.SinkOperator
import com.cloudminds.cdc.utils.TransformSchema
import org.apache.avro.Schema
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.apache.flink.table.catalog.ResolvedSchema
import org.apache.flink.table.types.DataType
import org.apache.flink.types.Row

import java.util

class SinkJdbc(confPath: String, tag: String, tableEnv: StreamTableEnvironment) extends SinkOperator{
  var jdbcSinkModel: JdbcSinkModel = _
  var table: Table = _

  override def getSinkProp(): Unit = {
    val propertiesFactory = new JdbcSinkPropertiesFactory(confPath)
    jdbcSinkModel = propertiesFactory.getProperties.get(tag)
  }

  override def transformTable(): Unit = {
    var transformSql: String = "select * from "+tag
    if (null != jdbcSinkModel && null != jdbcSinkModel.transformSql && jdbcSinkModel.transformSql.nonEmpty){
      val sql_arr: Array[String] = jdbcSinkModel.transformSql.split("\\s(?i)from\\s")
      transformSql = sql_arr(0).replaceAll("\\.", "_P_").replaceAll("\\$","_D_")+" from "+sql_arr(1)
    }
    // 执行sql转换操作
    //    tableEnv.getConfig.setSqlDialect(SqlDialect.HIVE)
    table = tableEnv.sqlQuery(transformSql)
  }

  override def executeSink(): Unit = {
    val dataSteam: DataStream[Row] = tableEnv.toChangelogStream(table)
    val resolvedSchema: ResolvedSchema = table.getResolvedSchema
    val jdbcSink: SinkFunction[Row] = getSink(resolvedSchema)
    dataSteam.addSink(jdbcSink).uid(tag+"sink-to-jdbc-table").name("sink to jdbc table")
  }

  def getSink(resolvedSchema: ResolvedSchema):SinkFunction[Row]={
    val insertSql: String = ConcatSql.getSql(jdbcSinkModel, resolvedSchema.getColumnNames)
//    val schema: Schema = AvroSchemaConverter.convertToSchema(resolvedSchema.toSourceRowDataType.getLogicalType)
    val transformSchema: util.LinkedHashMap[String, DataType] = TransformSchema.getColumnSchema(resolvedSchema.getColumns)
    val jdbcSink: SinkFunction[Row] = new JdbcSinkUtil(transformSchema, jdbcSinkModel).getSink(insertSql)
    jdbcSink
  }
}
