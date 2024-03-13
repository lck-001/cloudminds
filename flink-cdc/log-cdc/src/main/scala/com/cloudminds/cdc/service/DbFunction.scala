package com.cloudminds.cdc.service

import com.cloudminds.cdc.model.CommonProp
import org.apache.flink.streaming.api.datastream.DataStreamSource

import java.util

abstract class DbFunction {
//  createEnv()
  var commonProp:CommonProp
  var ds:DataStreamSource[String]
  protected def getProp(): Unit
  protected def getSchema(): Unit
  protected def getDs(): Unit
  def createEnv():Unit
/*  private def createEnv():Unit={
    getProp()
    getSchema()
    getDs()
  }*/
}
