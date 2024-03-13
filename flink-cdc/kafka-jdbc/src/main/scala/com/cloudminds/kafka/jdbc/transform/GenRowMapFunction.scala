package com.cloudminds.kafka.jdbc.transform

import com.alibaba.fastjson.JSONObject
import com.cloudminds.kafka.jdbc.jdbc.ExcludeColumns
import com.cloudminds.kafka.jdbc.schema.RowTypeTransform
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.types.Row

import java.util
import scala.collection.JavaConversions._


class GenRowMapFunction(schemaMap: util.HashMap[String, util.HashMap[String, String]]) extends RichMapFunction[JSONObject,Row]{
  override def map(json: JSONObject): Row = {
    getRow(json)
  }
  def getRow(json:JSONObject):Row={
    val db: String = json.getString("db")
    val table: String = json.getString("table")
    val schema: util.HashMap[String, String] = schemaMap.get(db + "." + table)
    val len: Int = json.size()
//    val row = new Row(len-ExcludeColumns.createMap().size())
    val row: Row = Row.withNames()
//    var i = 0
    for (entry <- json.entrySet) {
      if (!ExcludeColumns.createMap().containsKey(entry.getKey)){
//        row.setField(i,entry.getValue)
//        i+=1
        val dataType: String = schema.get(entry.getKey)
        val value: Object = RowTypeTransform.getDataType(dataType, entry.getValue)
        row.setField(entry.getKey,value)
      }
    }
    row
  }
}
