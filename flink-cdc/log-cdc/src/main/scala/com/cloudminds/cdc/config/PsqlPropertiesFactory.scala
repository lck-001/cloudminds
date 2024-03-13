package com.cloudminds.cdc.config

import com.cloudminds.cdc.model.source
import com.cloudminds.cdc.model.source.PsqlSourceModel
import io.debezium.connector.postgresql.PostgresConnectorConfig.SnapshotMode

import java.util
import scala.collection.mutable.ListBuffer

class PsqlPropertiesFactory(confPath: String) extends PropertiesFactory[PsqlSourceModel] {
  override def getProperties: PsqlSourceModel = {
    val propMap: util.HashMap[String, Object] = PropertiesUtils.getProperties(confPath).get("source").asInstanceOf[util.HashMap[String, Object]]
    val dataEnv: String = propMap.getOrDefault("dataEnv", "bj-prod-232").asInstanceOf[String]
    val hostname: String = propMap.getOrDefault("hostname", "").asInstanceOf[String]
    val port: Int = propMap.getOrDefault("port", "").asInstanceOf[Int]
    val username: String = propMap.getOrDefault("username", "").asInstanceOf[String]
    val password: String = propMap.getOrDefault("password", "").asInstanceOf[String]
    val slotName: String = propMap.getOrDefault("slotName", "").asInstanceOf[String]
    val snapshotMd: String = propMap.getOrDefault("snapshotMode", "initial").asInstanceOf[String]
    val snapshotMode: SnapshotMode = snapshotMd.toLowerCase.trim match {
      case "initial" => SnapshotMode.INITIAL
      case "always" => SnapshotMode.ALWAYS
      case "never" => SnapshotMode.NEVER
      case "initial_only" => SnapshotMode.INITIAL_ONLY
      case "exported" => SnapshotMode.EXPORTED
      case "custom" => SnapshotMode.CUSTOM
    }
    val dataList: util.ArrayList[util.HashMap[String, String]] = propMap.get("data").asInstanceOf[util.ArrayList[util.HashMap[String, String]]]
    val dataTuple: (ListBuffer[String], ListBuffer[String], ListBuffer[String], ListBuffer[String], util.HashMap[String, String]) = getDbTables(dataList)
    PsqlSourceModel(excludeColumns = dataTuple._5, dataEnv = dataEnv, hostname = hostname, port = port, username = username, password = password, slotName = slotName, snapshotMode = snapshotMode, databases = dataTuple._1, schemas = dataTuple._2, schemaTables = dataTuple._3, dbTables = dataTuple._4)
  }

  private def getDbTables(dataList: util.ArrayList[util.HashMap[String, String]]): (ListBuffer[String], ListBuffer[String], ListBuffer[String], ListBuffer[String], util.HashMap[String, String]) = {
    val dbs = new ListBuffer[String]
    val sch = new ListBuffer[String]
    val tabs = new ListBuffer[String]
    val dbTabs = new ListBuffer[String]
    val excludeMap = new util.HashMap[String, String]()
    for (data <- dataList.toArray) {
      val dataMap = data.asInstanceOf[util.HashMap[String, String]]
      val excludeColumns: String = dataMap.getOrDefault("excludeColumns", "")
      val database = dataMap.getOrDefault("database", "")
      dbs.append(database)
      val schema = dataMap.getOrDefault("schema", "public")
      sch.append(schema)
      val tableList = dataMap.getOrDefault("tables", "")
      for (table <- tableList.split(",")) {
        if (table.nonEmpty) {
          tabs.append(schema + "." + table)
          dbTabs.append(database + "." + table)
          excludeMap.put(database + "." + table, excludeColumns)
        }
      }
    }
    val databases = dbs.filter(s => s.nonEmpty)
    val schemas = sch.filter(s => s.nonEmpty)
    val schemaTables = tabs.filter(s => s.nonEmpty)
    val dbTables = dbTabs.filter(s => s.nonEmpty)
    (databases, schemas, schemaTables, dbTables, excludeMap)
  }
}
