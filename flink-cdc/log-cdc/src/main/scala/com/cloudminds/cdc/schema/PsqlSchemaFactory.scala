package com.cloudminds.cdc.schema
import com.cloudminds.cdc.config.PsqlPropertiesFactory
import com.cloudminds.cdc.jdbc.JdbcTools
import com.cloudminds.cdc.model.source.PsqlSourceModel
import org.apache.avro.Schema

import java.util
import scala.collection.mutable.ListBuffer

class PsqlSchemaFactory(model: PsqlSourceModel) extends SchemaFactory {
  override def createSchema: util.HashMap[String, String] = {
    val excludeColumnsMap: util.HashMap[String, String] = model.excludeColumns
    val dbTables: ListBuffer[String] = model.dbTables
    val psqlSchema = new util.HashMap[String,String]
    for (elem <- dbTables){
      val dbTable: Array[String] = elem.split("\\.")
      // 需要判断db和table是否是带中横线的业务库和表,如果是需要对使用引号转义
      val db: String = if (dbTable(0).contains("-")) "`" + dbTable(0) + "`" else dbTable(0)
      val table: String = dbTable(1)
      val url: String = "jdbc:postgresql://" + model.hostname + ":" + model.port + "/" + db + "?createDatabaseIfNotExist=true&useSSL=false"
      val username: String = model.username
      val password: String = model.password
      val tool = new JdbcTools("mysql", db, table, url, username, password)
      val excludeColumns: String = excludeColumnsMap.get(elem)
      val schema: Schema = tool.executeSql(excludeColumns)
      psqlSchema.put(elem,schema.toString)
    }
    psqlSchema
  }
}
