package com.cloudminds.cdc.transform

import com.cloudminds.cdc.jdbc.ExcludeColumns
import com.cloudminds.cdc.utils.SqlParserHandler
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.table.catalog.{Column, ResolvedSchema}
import org.apache.flink.table.types.DataType
import org.apache.flink.types.Row

import java.util
import java.util.HashMap
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

class FilterRowMapFunction(transformSchema: util.LinkedHashMap[String, DataType]) extends RichMapFunction[Row, Row] {
  override def map(row: Row): Row = {
/*    val fieldNames: util.Set[String] = row.getFieldNames(true)
    val filterRow = new Row(fieldNames.size()-ExcludeColumns.excludeColumnsMap.size())
    val it: util.Iterator[String] = fieldNames.iterator()
    var i = 0
    while (it.hasNext){
      val field: String = it.next()
      if (!ExcludeColumns.excludeColumnsMap.containsKey(field)){
        val fieldValue: Object = row.getField(field)
        filterRow.setField(i,fieldValue)
        i+=1
      }

    }
    filterRow*/
    val arr: ArrayBuffer[String] = ArrayBuffer[String]()
/*    for(column <- transformSchema.getList){
      if (!ExcludeColumns.excludeColumnsMap.containsKey(column.getName)){
        arr.append(column.getName)
      }
    }*/
    for (key <- transformSchema.keySet()){
      if (!ExcludeColumns.excludeColumnsMap.containsKey(key)){
        arr.append(key)
      }
    }
    val filterRow: Row = Row.project(row, arr.toArray)
    filterRow
  }

/*  def project(row:Row,fields:Array[Int]): Row ={
    val copyRow = new Row(fields.length)
    for (i <- fields.indices){

    }
  }*/

}
