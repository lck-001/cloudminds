package com.cloudminds.cdc.model

import com.alibaba.fastjson.JSONObject

import java.util

//case class CommonSource(db:String,table:String,bigdata_method:String,event_time:Long,schema:util.HashMap[String, String],before:JSONObject,after:JSONObject)
// schema -> fieldName,fieldType
case class CommonSource(var dbTable: String, dataEnv: String, var bigdata_method: String, var event_time: Long,schema: util.HashMap[String, String], var data: String)
