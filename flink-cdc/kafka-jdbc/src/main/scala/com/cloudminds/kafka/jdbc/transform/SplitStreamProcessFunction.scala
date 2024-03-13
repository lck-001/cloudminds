package com.cloudminds.kafka.jdbc.transform

import com.alibaba.fastjson.JSONObject
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.types.Row
import org.apache.flink.util.{Collector, OutputTag}
import scala.collection.JavaConversions._
import java.util

class SplitStreamProcessFunction(tagMap:util.HashMap[String, OutputTag[JSONObject]]) extends ProcessFunction[JSONObject,JSONObject]{
  override def processElement(json: JSONObject, ctx: ProcessFunction[JSONObject, JSONObject]#Context, out: Collector[JSONObject]): Unit = {
    val db: String = json.getString("db")
    val table: String = json.getString("table")
    val outputTag: OutputTag[JSONObject] = tagMap.get(db + "." + table)
    if (outputTag != null){
      ctx.output(outputTag,json)
    }
  }
  /*def getRow(json:JSONObject):Row={
    val len: Int = json.size()
    val row = new Row(len)
    var i = 0
    for (entry <- json.entrySet) {
      row.setField(i,entry.getValue)
      i+=1
    }
    row
  }*/
}
