package com.cloudminds.cdc.transform

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.functions.RichMapFunction

class GenJsonMapFunction extends RichMapFunction[GenericRecord,JSONObject]{
  override def map(record: GenericRecord): JSONObject = {
    val json = JSON.parseObject(record.toString)
    json
  }
}
