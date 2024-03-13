package com.cloudminds.cdc.model

import java.util
// dataSchema -> tableName,schemaStr
case class CommonProp(
  var sourceType:String,
  var dataEnv:String,
  var dataSchema:util.HashMap[String,String],
  var tableRule:util.HashMap[String,String]
  )
