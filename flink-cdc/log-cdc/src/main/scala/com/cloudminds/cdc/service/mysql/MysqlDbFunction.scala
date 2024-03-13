package com.cloudminds.cdc.service.mysql

import com.cloudminds.cdc.config.{MysqlPropertiesFactory, PropertiesFactory}
import com.cloudminds.cdc.model.CommonProp
import com.cloudminds.cdc.model.source.MysqlSourceModel
import com.cloudminds.cdc.schema.{MysqlSchemaFactory, SchemaFactory}
import com.cloudminds.cdc.service.{DbFunction, SourceFactory}
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

import java.util

class MysqlDbFunction(confPath:String,env: StreamExecutionEnvironment) extends DbFunction {
  override var commonProp: CommonProp = CommonProp("mysql","bj-prod-232",new util.HashMap[String,String](),new util.HashMap[String,String]())
  override var ds: DataStreamSource[String] = _

  var mysqlSourceModel: MysqlSourceModel = _
  private var propFactory:PropertiesFactory[MysqlSourceModel] = _
  private var schemaFactory:SchemaFactory = _
  private var sourceFactory:SourceFactory = _
  override protected def getProp():Unit = {
    propFactory = new MysqlPropertiesFactory(confPath)
    mysqlSourceModel = propFactory.getProperties
    commonProp.dataEnv = propFactory.getProperties.dataEnv
  }

  override protected def getSchema(): Unit = {
    schemaFactory = new MysqlSchemaFactory(mysqlSourceModel)
    commonProp.dataSchema = schemaFactory.createSchema
  }

  override protected def getDs(): Unit = {
    sourceFactory = new MysqlSourceFactory(mysqlSourceModel)
    ds = sourceFactory.createSource(env)
  }

  override def createEnv: Unit = {
    getProp()
    getSchema()
    getDs()
  }
}
