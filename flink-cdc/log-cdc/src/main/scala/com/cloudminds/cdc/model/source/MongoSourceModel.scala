package com.cloudminds.cdc.model.source

import java.util
import scala.collection.mutable.ListBuffer
// tableSchema -> tableName,fieldName,fieldType
case class MongoSourceModel(dataEnv: String, hostname: String, port: Int, username: String, password: String, databases: ListBuffer[String],
                            dbTables: ListBuffer[String], tableSchema: util.HashMap[String, util.HashMap[String, String]])
