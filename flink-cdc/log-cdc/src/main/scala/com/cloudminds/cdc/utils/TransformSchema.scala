package com.cloudminds.cdc.utils

import org.apache.flink.table.catalog.Column
import org.apache.flink.table.types.DataType
import scala.collection.JavaConversions._
import java.util
// 获取转换操作后的table schema
object TransformSchema extends Serializable {
  def getColumnSchema(list: util.List[Column]): util.LinkedHashMap[String, DataType] = {
    val map = new util.LinkedHashMap[String, DataType]
    for (column <- list){
      map.put(column.getName,column.getDataType)
    }
    map
  }
}
