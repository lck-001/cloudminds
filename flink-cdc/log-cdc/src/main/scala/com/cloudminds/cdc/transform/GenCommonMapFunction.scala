/*
package com.cloudminds.cdc.transform

import com.alibaba.fastjson.JSONObject
import com.cloudminds.cdc.model.CommonSource
import org.apache.flink.api.common.functions.RichMapFunction

import java.util

class GenCommonMapFunction(dataEnv : String) extends RichMapFunction[JSONObject,CommonSource]{
  override def map(in: JSONObject): CommonSource = {
    val db = in.getString("db")
    val table = in.getString("table")
    val bigdata_method = in.getString("bigdata_method").toLowerCase()
    val event_time = in.getLong("event_time")
    val schema = in.getObject("schema", classOf[util.HashMap[String, String]])
    val before = in.getJSONObject("before")
    if (before != null){
      before.put("k8s_env_name",dataEnv)
    }
    val after = in.getJSONObject("after")
    if (after != null){
      after.put("k8s_env_name",dataEnv)
    }
    CommonSource(db = db, table = table, bigdata_method = bigdata_method, event_time = event_time, schema = schema, before = before, after = after)
  }
}
*/
