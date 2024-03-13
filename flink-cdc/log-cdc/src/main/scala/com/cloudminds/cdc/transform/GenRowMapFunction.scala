package com.cloudminds.cdc.transform

import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.types.Row

class GenRowMapFunction extends RichMapFunction[GenericRecord,Row]{
  override def map(genericRecord: GenericRecord): Row = {
    val columnNum: Int = genericRecord.getSchema.getFields.size()
//    val rowData = new Array[Object](columnNum)
    val row = new Row(columnNum)
    for ( i <- 0 until columnNum){
//      rowData(i) = genericRecord.get(i)
      row.setField(i,genericRecord.get(i))
    }
    row
  }
}
