package com.cloudminds.cdc.service.sink

import com.cloudminds.cdc.config.HbaseSinkPropertiesFactory
import com.cloudminds.cdc.model.sink.HbaseSinkModel
import com.cloudminds.cdc.service.SinkOperator
import org.apache.avro.Schema
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter
import org.apache.flink.table.api.{SqlDialect, StatementSet, Table, TableResult}
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment

class SinkHbase(confPath: String, tag: String, tableEnv: StreamTableEnvironment,statementSet: StatementSet) extends SinkOperator {
  var hbaseSinkModel: HbaseSinkModel = _

  override def getSinkProp(): Unit = {
    val factory = new HbaseSinkPropertiesFactory(confPath)
    hbaseSinkModel = factory.getProperties.get(tag)
  }

  override def transformTable(): Unit = {
    System.setProperty("zookeeper.sasl.client", "false");
    assert(null != hbaseSinkModel && null != hbaseSinkModel.registerSql && hbaseSinkModel.registerSql.nonEmpty,"register table sql must not null")
//    val table: Table = tableEnv.sqlQuery(hbaseSinkModel.sinkSql)
//    val schema: Schema = AvroSchemaConverter.convertToSchema(table.getResolvedSchema.toSourceRowDataType.getLogicalType)
//    schema
    tableEnv.executeSql(hbaseSinkModel.registerSql)
  }

  override def executeSink(): Unit = {
    System.setProperty("zookeeper.sasl.client", "false");
//    val statementSet: StatementSet = tableEnv.createStatementSet

    val sinkSql: String = hbaseSinkModel.sinkSql
    statementSet.addInsertSql(sinkSql)
/*    val result: TableResult = statementSet.execute()
    println(result.getJobClient.get().getJobStatus)*/
  }
}
