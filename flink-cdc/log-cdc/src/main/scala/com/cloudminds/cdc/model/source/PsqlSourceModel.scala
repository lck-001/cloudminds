package com.cloudminds.cdc.model.source

import io.debezium.connector.postgresql.PostgresConnectorConfig.SnapshotMode

import java.util
import scala.collection.mutable.ListBuffer

case class PsqlSourceModel(excludeColumns: util.HashMap[String, String], dataEnv: String, hostname: String,
                           port: Int, username: String, password: String, slotName: String, snapshotMode: SnapshotMode,databases: ListBuffer[String],
                           schemas: ListBuffer[String], schemaTables: ListBuffer[String], dbTables: ListBuffer[String])
