package com.cloudminds.cdc.config

import com.cloudminds.cdc.model.sink.HdfsSinkModel

import scala.collection.JavaConversions._
import java.util
import scala.collection.mutable.ListBuffer

class HdfsSinkPropertiesFactory(confPath:String) extends PropertiesFactory[util.HashMap[String,HdfsSinkModel]]{
  override def getProperties: util.HashMap[String,HdfsSinkModel] = {
    val propMap: util.HashMap[String, Object] = PropertiesUtils.getProperties(confPath).get("sink").asInstanceOf[util.HashMap[String,Object]]
    val sinkHdfsList: util.ArrayList[util.HashMap[String, String]] = propMap.get("sinkHdfs").asInstanceOf[util.ArrayList[util.HashMap[String, String]]]
    val hdfsSinkMap = new util.HashMap[String,HdfsSinkModel]
    if (null != sinkHdfsList && sinkHdfsList.nonEmpty) {
      for (sinkHdfs <- sinkHdfsList ){
        val dbTable: String = sinkHdfs.get("dbTable")
        val transformSql: String = Option(sinkHdfs.get("transformSql")).getOrElse("")
        val partitions: String = Option(sinkHdfs.get("partitions")).getOrElse("")
        val hdfsPath: String = sinkHdfs.get("hdfsPath")
        hdfsSinkMap.put(dbTable,HdfsSinkModel(dbTable = dbTable, transformSql = transformSql, hdfsPath = hdfsPath, partitions = partitions))
      }
    }
    hdfsSinkMap
  }
}
