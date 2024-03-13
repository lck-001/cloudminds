package com.cloudminds.cdc.config

import com.cloudminds.cdc.model.sink.HbaseSinkModel

import scala.collection.JavaConversions._
import java.util

class HbaseSinkPropertiesFactory (confPath:String) extends PropertiesFactory[util.HashMap[String,HbaseSinkModel]]{
  override def getProperties: util.HashMap[String, HbaseSinkModel] = {
    val propMap: util.HashMap[String, Object] = PropertiesUtils.getProperties(confPath).get("sink").asInstanceOf[util.HashMap[String,Object]]
    val sinkHbaseList: util.ArrayList[util.HashMap[String, String]] = propMap.get("sinkHbase").asInstanceOf[util.ArrayList[util.HashMap[String, String]]]
    val hbaseSinkMap = new util.HashMap[String, HbaseSinkModel]
    if (null != sinkHbaseList && !sinkHbaseList.isEmpty){
      for (sinkHbase <- sinkHbaseList){
        val dbTable: String = sinkHbase.get("dbTable")
        val registerSql: String = sinkHbase.get("registerSql")
        val sinkSql: String = sinkHbase.get("sinkSql")
        hbaseSinkMap.put(dbTable,HbaseSinkModel(dbTable = dbTable, registerSql = registerSql, sinkSql = sinkSql))
      }
    }
    hbaseSinkMap
  }
}
