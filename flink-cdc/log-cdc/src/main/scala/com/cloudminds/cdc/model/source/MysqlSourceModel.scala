package com.cloudminds.cdc.model.source

import io.debezium.connector.mysql.MySqlConnectorConfig.SnapshotMode

import java.util
import scala.collection.mutable.ListBuffer

case class MysqlSourceModel(excludeColumns: util.HashMap[String, String], dataEnv: String, hostname: String,
                            port: Int, username: String, password: String, serverId: String, snapshotMode: SnapshotMode, databases: ListBuffer[String], dbTables: ListBuffer[String])
