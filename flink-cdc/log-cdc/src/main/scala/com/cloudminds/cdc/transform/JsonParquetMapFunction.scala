package com.cloudminds.cdc.transform

import com.cloudminds.cdc.model.JsonParquet
import org.apache.flink.api.common.functions.RichMapFunction

class JsonParquetMapFunction extends RichMapFunction[String,JsonParquet]{
  override def map(data: String): JsonParquet = {
    JsonParquet(data)
  }
}
