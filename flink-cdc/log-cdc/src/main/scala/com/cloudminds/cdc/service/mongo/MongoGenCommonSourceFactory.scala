package com.cloudminds.cdc.service.mongo

import com.alibaba.fastjson.serializer.SerializerFeature
import com.alibaba.fastjson.{JSON, JSONObject}
import com.cloudminds.cdc.model.{CommonProp, CommonSource}
import com.cloudminds.cdc.service.GenCommonSourceFactory

import java.util

class MongoGenCommonSourceFactory(commonProp: CommonProp) extends GenCommonSourceFactory {
  override def getCommonSource(str: String): CommonSource = {
    val jsonObject: JSONObject = JSON.parseObject(str)
    val payload: JSONObject = jsonObject.getJSONObject("payload")
    var fullDocument: String = payload.getString("fullDocument")
    if (null == fullDocument || fullDocument.isEmpty) {
      fullDocument = new JSONObject().toString
    }
    val dataJson: JSONObject = JSON.parseObject(fullDocument)
    val ns: JSONObject = payload.getJSONObject("ns")
    val db: String = ns.getString("db")

    val table: String = ns.getString("coll")
    val dbTable: String = db + "." + table
    val operationType: String = payload.getString("operationType")
    val bigdata_method: String = operationType.toLowerCase match {
      case "insert" => "i"
      case "delete" => "d"
      case "update" => "u"
      case "replace" => "u"
      case _ => operationType
    }
    CommonSource(dbTable = dbTable, dataEnv = commonProp.dataEnv, bigdata_method = bigdata_method, event_time = System.currentTimeMillis(), schema = new util.HashMap[String, String](), data = JSON.toJSONString(dataJson, SerializerFeature.WriteMapNullValue))
  }
}
