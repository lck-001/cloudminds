package com.cloudminds.cdc.config

import com.cloudminds.cdc.model.sink.JdbcSinkModel

import scala.collection.JavaConversions._
import java.util

class JdbcSinkPropertiesFactory(confPath: String) extends PropertiesFactory[util.HashMap[String, JdbcSinkModel]] {
  override def getProperties: util.HashMap[String, JdbcSinkModel] = {
    val propMap: util.HashMap[String, Object] = PropertiesUtils.getProperties(confPath).get("sink").asInstanceOf[util.HashMap[String, Object]]
    val sinkJdbcList: util.ArrayList[util.HashMap[String, Object]] = propMap.get("sinkJdbc").asInstanceOf[util.ArrayList[util.HashMap[String, Object]]]
    val jdbcSinkMap = new util.HashMap[String, JdbcSinkModel]
    if (null != sinkJdbcList && sinkJdbcList.nonEmpty) {
      for (sinkJdbc <- sinkJdbcList ) {
        val dbTable: String = sinkJdbc.get("dbTable").toString
        val jdbcUrl: String = sinkJdbc.get("jdbcUrl").toString
        val jdbcDriver: String = sinkJdbc.get("jdbcDriver").toString
        val jdbcType: String = sinkJdbc.get("jdbcType").toString
        val username: String = Option(sinkJdbc.get("username")).getOrElse("").toString
        val password: String = Option(sinkJdbc.get("password")).getOrElse("").toString
        val batchSize: Int = sinkJdbc.getOrDefault("batchSize", "100").toString.toInt
        val maxRetries: Int = sinkJdbc.getOrDefault("maxRetries", "3").toString.toInt
        val batchInterval: Int = sinkJdbc.getOrDefault("batchInterval", "30000").toString.toInt
        val transformSql: String = Option(sinkJdbc.get("transformSql")).getOrElse("").toString
        val sinkDatabase: String = sinkJdbc.get("sinkDatabase").toString
        val sinkTable: String = sinkJdbc.get("sinkTable").toString
        jdbcSinkMap.put(dbTable, JdbcSinkModel(dbTable = dbTable, jdbcType = jdbcType, transformSql = transformSql, jdbcUrl = jdbcUrl, jdbcDriver = jdbcDriver, username = username, password = password, batchSize = batchSize, maxRetries = maxRetries, batchInterval = batchInterval, sinkDatabase = sinkDatabase, sinkTable = sinkTable))
      }
    }
    jdbcSinkMap
  }
}
