package com.cloudminds.cdc.service.mqtt

import com.cloudminds.cdc.config.{MqttPropertiesFactory, PropertiesFactory}
import com.cloudminds.cdc.model.CommonProp
import com.cloudminds.cdc.model.source.MqttSourceModel
import com.cloudminds.cdc.schema.{MqttSchemaFactory, SchemaFactory}
import com.cloudminds.cdc.service.{DbFunction, SourceFactory}
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

import java.util

class MqttDbFunction (confPath:String,env: StreamExecutionEnvironment) extends DbFunction{
  override var commonProp: CommonProp = CommonProp("mqtt","bj-prod-232",new util.HashMap[String,String](),new util.HashMap[String,String]())
  override var ds: DataStreamSource[String] = _
  var mqttSourceModel:MqttSourceModel = _
  private var propFactory:PropertiesFactory[MqttSourceModel] = _
  private var schemaFactory:SchemaFactory = _
  private var sourceFactory:SourceFactory = _

  override protected def getProp(): Unit = {
    propFactory = new MqttPropertiesFactory(confPath)
    mqttSourceModel = propFactory.getProperties
    commonProp.tableRule = mqttSourceModel.tableRule
    commonProp.dataEnv = propFactory.getProperties.dataEnv
  }

  override protected def getSchema(): Unit = {
    schemaFactory = new MqttSchemaFactory(mqttSourceModel)
    commonProp.dataSchema = schemaFactory.createSchema
  }

  override protected def getDs(): Unit = {
    sourceFactory = new MqttSourceFactory(mqttSourceModel)
    ds = sourceFactory.createSource(env)
  }

  override def createEnv(): Unit = {
    getProp()
    getSchema()
    getDs()
  }
}
