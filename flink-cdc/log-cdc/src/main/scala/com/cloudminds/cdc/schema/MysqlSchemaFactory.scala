package com.cloudminds.cdc.schema

import com.cloudminds.cdc.config.{MysqlPropertiesFactory, PropertiesFactory}
import com.cloudminds.cdc.jdbc.JdbcTools
import com.cloudminds.cdc.model.source.MysqlSourceModel
import org.apache.avro.Schema

import java.util
import scala.collection.mutable.ListBuffer

class MysqlSchemaFactory(model: MysqlSourceModel) extends SchemaFactory {
  override def createSchema: util.HashMap[String, String] = {
    val excludeColumnsMap: util.HashMap[String, String] = model.excludeColumns
    val dbTables: ListBuffer[String] = model.dbTables
    val mysqlSchema = new util.HashMap[String, String]()
    for (elem <- dbTables if dbTables.nonEmpty) {
      val dbTable: Array[String] = elem.split("\\.")
      // 需要判断db和table是否是带中横线的业务库和表,如果是需要对使用引号转义
      val db: String = if (dbTable(0).contains("-")) "`" + dbTable(0) + "`" else dbTable(0)
      val table: String = dbTable(1)
      val url: String = "jdbc:mysql://" + model.hostname + ":" + model.port + "/" + db + "?createDatabaseIfNotExist=true&useSSL=false"
      val username: String = model.username
      val password: String = model.password
      val tool = new JdbcTools("mysql", db, table, url, username, password)
      val excludeColumns: String = excludeColumnsMap.get(elem)
      val schema: Schema = tool.executeSql(excludeColumns)
      mysqlSchema.put(elem, schema.toString)
    }
    mysqlSchema
  }
}