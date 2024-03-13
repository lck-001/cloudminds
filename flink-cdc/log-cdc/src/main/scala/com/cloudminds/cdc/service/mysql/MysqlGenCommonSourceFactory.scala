package com.cloudminds.cdc.service.mysql

import com.alibaba.fastjson.serializer.SerializerFeature
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.cloudminds.cdc.model.{CommonProp, CommonSource}
import com.cloudminds.cdc.service.GenCommonSourceFactory

import java.{lang, util}

class MysqlGenCommonSourceFactory(commonProp: CommonProp) extends GenCommonSourceFactory {
  override def getCommonSource(str: String): CommonSource = {
    val jsonObject: JSONObject = JSON.parseObject(str)
    val payload: JSONObject = jsonObject.getJSONObject("payload")
    val source: JSONObject = payload.getJSONObject("source")
    // 获取数据库
    val db: String = source.getString("db")
    // 获取表
    val table: String = source.getString("table")
    // 操作类型
    val op: String = payload.getString("op")
    // 获取oplog抓取时间
    val event_time: lang.Long = payload.getLongValue("ts_ms").asInstanceOf[java.lang.Long]
    // 获取schema
    val schema: JSONObject = jsonObject.getJSONObject("schema")
    val fields: JSONArray = schema.getJSONArray("fields")
    var fieldMap = new util.HashMap[String,String]()
    if (op.equalsIgnoreCase("d")){
      // 拿出删除之前的数据
      val dataJson: JSONObject = payload.getJSONObject("before")

      dataJson.putIfAbsent("event_time",event_time)
      dataJson.putIfAbsent("bigdata_method",op)
      dataJson.putIfAbsent("k8s_env_name",commonProp.dataEnv)
      val before: JSONObject = fields.getJSONObject(0)
      fieldMap = getJsonArr(before)
      CommonSource(db+"."+table,commonProp.dataEnv,op,event_time,fieldMap,JSON.toJSONString(dataJson,SerializerFeature.WriteMapNullValue))
    }else{
      // 拿出变更之后的数据
      val dataJson: JSONObject = payload.getJSONObject("after")
      dataJson.putIfAbsent("event_time",event_time)
      dataJson.putIfAbsent("bigdata_method",op)
      dataJson.putIfAbsent("k8s_env_name",commonProp.dataEnv)
      val after: JSONObject = fields.getJSONObject(1)
      fieldMap = getJsonArr(after)
      CommonSource(db+"."+table,commonProp.dataEnv,op,event_time,fieldMap,JSON.toJSONString(dataJson,SerializerFeature.WriteMapNullValue))
    }
  }
  def getJsonArr(json:JSONObject):util.HashMap[String, String]={
    val fieldMap = new util.HashMap[String, String]()
    val fields: JSONArray = json.getJSONArray("fields")
    for (arr <- fields.toArray){
      val fieldObj: JSONObject = arr.asInstanceOf[JSONObject]
      val field: String = fieldObj.getString("field")
      val fieldType: String = fieldObj.getString("type")
      fieldMap.put(field,fieldType)
    }
    fieldMap
  }
}
