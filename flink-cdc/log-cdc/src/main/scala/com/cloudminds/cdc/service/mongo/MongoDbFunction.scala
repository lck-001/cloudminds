package com.cloudminds.cdc.service.mongo

import com.cloudminds.cdc.config.{MongoPropertiesFactory, PropertiesFactory}
import com.cloudminds.cdc.model.CommonProp
import com.cloudminds.cdc.model.source.MongoSourceModel
import com.cloudminds.cdc.schema.{MongoSchemaFactory, SchemaFactory}
import com.cloudminds.cdc.service.{DbFunction, SourceFactory}
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

import java.util

class MongoDbFunction(confPath:String,env: StreamExecutionEnvironment) extends DbFunction {
  override var commonProp: CommonProp = CommonProp("mongo","bj-prod-232",new util.HashMap[String,String](),new util.HashMap[String,String]())
  override var ds: DataStreamSource[String] = _

  var mongoSourceModel: MongoSourceModel = _
  private var propFactory:PropertiesFactory[MongoSourceModel] = _
  private var schemaFactory:SchemaFactory = _
  private var sourceFactory:SourceFactory = _
  override protected def getProp(): Unit = {
    propFactory = new MongoPropertiesFactory(confPath)
    mongoSourceModel = propFactory.getProperties
    commonProp.dataEnv = mongoSourceModel.dataEnv
  }

  override protected def getSchema(): Unit = {
    schemaFactory = new MongoSchemaFactory(mongoSourceModel)
    commonProp.dataSchema = schemaFactory.createSchema
  }

  override protected def getDs(): Unit = {
    sourceFactory = new MongoSourceFactory(mongoSourceModel)
    ds = sourceFactory.createSource(env)
  }

  override def createEnv: Unit = {
    getProp()
    getSchema()
    getDs()
  }
}
