package com.cloudminds.cdc.service.psql

import com.cloudminds.cdc.config.{PropertiesFactory, PsqlPropertiesFactory}
import com.cloudminds.cdc.model.CommonProp
import com.cloudminds.cdc.model.source.PsqlSourceModel
import com.cloudminds.cdc.schema.{PsqlSchemaFactory, SchemaFactory}
import com.cloudminds.cdc.service.{DbFunction, SourceFactory}
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

import java.util


class PsqlDbFunction(confPath:String,env: StreamExecutionEnvironment) extends DbFunction {
  override var commonProp: CommonProp = CommonProp("postgre","bj-prod-232",new util.HashMap[String,String](),new util.HashMap[String,String]())
  override var ds: DataStreamSource[String] = _
  var psqlSourceModel: PsqlSourceModel = _
  private var propFactory:PropertiesFactory[PsqlSourceModel] = _
  private var schemaFactory:SchemaFactory = _
  private var sourceFactory:SourceFactory = _
  override protected def getProp(): Unit = {
    propFactory = new PsqlPropertiesFactory(confPath)
    psqlSourceModel = propFactory.getProperties

    commonProp.dataEnv = propFactory.getProperties.dataEnv
  }

  override protected def getSchema(): Unit = {
    schemaFactory = new PsqlSchemaFactory(psqlSourceModel)
    commonProp.dataSchema = schemaFactory.createSchema
  }

  override protected def getDs(): Unit = {
    sourceFactory = new PsqlSourceFactory(psqlSourceModel)
    ds = sourceFactory.createSource(env)
  }

  override def createEnv: Unit = {
    getProp()
    getSchema()
    getDs()
  }


}
