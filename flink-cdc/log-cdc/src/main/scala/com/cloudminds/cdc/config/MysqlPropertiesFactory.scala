package com.cloudminds.cdc.config

import com.cloudminds.cdc.model.source
import com.cloudminds.cdc.model.source.MysqlSourceModel
import io.debezium.connector.mysql.MySqlConnectorConfig.SnapshotMode

import java.util
import scala.collection.mutable.ListBuffer


class MysqlPropertiesFactory(confPath: String) extends PropertiesFactory[MysqlSourceModel] {

  override def getProperties: MysqlSourceModel = {
    val propMap: util.HashMap[String, Object] = PropertiesUtils.getProperties(confPath).get("source").asInstanceOf[util.HashMap[String, Object]]
    val dataEnv: String = propMap.getOrDefault("dataEnv", "bj-prod-232").asInstanceOf[String]
    val hostname: String = propMap.getOrDefault("hostname", "").asInstanceOf[String]
    val port: Int = propMap.getOrDefault("port", "3306").asInstanceOf[Int]
    val username: String = propMap.getOrDefault("username", "").asInstanceOf[String]
    val password: String = propMap.getOrDefault("password", "").asInstanceOf[String]
    val serverId: String = propMap.getOrDefault("serverId", "").toString
    val snapshotMd: String = propMap.getOrDefault("snapshotMode", "when_needed").asInstanceOf[String]
    val dataList: util.ArrayList[util.HashMap[String, String]] = propMap.get("data").asInstanceOf[util.ArrayList[util.HashMap[String, String]]]
    val dataTuple: (ListBuffer[String], ListBuffer[String], ListBuffer[String], util.HashMap[String, String]) = getDbTables(dataList)
    val snapshotMode: SnapshotMode = snapshotMd.toLowerCase.trim match {
      case "initial" => SnapshotMode.INITIAL
      case "initial_only" => SnapshotMode.INITIAL_ONLY
      case "schema_only" => SnapshotMode.SCHEMA_ONLY
      case "schema_only_recovery" => SnapshotMode.SCHEMA_ONLY_RECOVERY
      case "NEVER" => SnapshotMode.NEVER
      case _ => SnapshotMode.WHEN_NEEDED
    }
    MysqlSourceModel(excludeColumns = dataTuple._4, dataEnv = dataEnv, hostname = hostname, port = port, username = username, password = password, serverId = serverId, snapshotMode = snapshotMode, databases = dataTuple._1, dbTables = dataTuple._3)
  }

  private def getDbTables(dataList: util.ArrayList[util.HashMap[String, String]]): (ListBuffer[String], ListBuffer[String], ListBuffer[String], util.HashMap[String, String]) = {
    val dbs = new ListBuffer[String]
    val dbTabs = new ListBuffer[String]
    val tables = new ListBuffer[String]
    val excludeMap = new util.HashMap[String, String]()
    for (data <- dataList.toArray if null != dataList && !dataList.isEmpty) {
      val dataMap = data.asInstanceOf[util.HashMap[String, String]]
      val excludeColumns: String = dataMap.getOrDefault("excludeColumns", "")
      val database = dataMap.getOrDefault("database", "")
      dbs.append(database)
      val tableList = dataMap.getOrDefault("tables", "")
      for (table <- tableList.split(",")) {
        if (table.nonEmpty) {
          // tabs 已经拼接成 db.table
          dbTabs.append(database + "." + table)
          tables.append(table)
          excludeMap.put(database + "." + table, excludeColumns)
        }
      }
    }
    val databases = dbs.filter(s => s.nonEmpty)
    val dbTables: ListBuffer[String] = dbTabs.filter(s => s.nonEmpty)
    (databases, tables, dbTables, excludeMap)
  }
}
