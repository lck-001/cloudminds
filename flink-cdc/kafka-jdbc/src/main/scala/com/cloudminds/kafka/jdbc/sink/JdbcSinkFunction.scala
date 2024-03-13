package com.cloudminds.kafka.jdbc.sink

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.configuration.{ConfigOptions, Configuration}
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.types.Row

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util

class JdbcSinkFunction extends RichSinkFunction[Row]{
  //定义sql连接、预编译器
  var conn: Connection = _
  var state: PreparedStatement = _
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    val globalJobParameters: ExecutionConfig.GlobalJobParameters = getRuntimeContext.getExecutionConfig.getGlobalJobParameters
    val globConf: Configuration = globalJobParameters.asInstanceOf[Configuration]
    val jdbcDriver = globConf.getString(ConfigOptions.key("jdbcDriver").stringType().defaultValue(""))
    val jdbcUrl = globConf.getString(ConfigOptions.key("jdbcUrl").stringType().defaultValue(""))
    val jdbcUsername = globConf.getString(ConfigOptions.key("jdbcUsername").stringType().defaultValue(""))
    val jdbcPassword = globConf.getString(ConfigOptions.key("jdbcPassword").stringType().defaultValue(""))
    Class.forName(jdbcDriver)
    conn = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword)
  }

  override def invoke(row: Row, context: SinkFunction.Context): Unit = {
    val fields: util.Set[String] = row.getFieldNames(true)
    // 拼装sql 使用map缓存起来，避免根据每条数据都生成一次sql语句
    var sql = "insert into "
    // 先获取对应的sql 预编译，然后在执行ps.addBatch批量添加
    conn.prepareStatement(sql)
    println(fields.size())
  }

  override def close(): Unit = {
    state.close()
    conn.close()
  }
}
