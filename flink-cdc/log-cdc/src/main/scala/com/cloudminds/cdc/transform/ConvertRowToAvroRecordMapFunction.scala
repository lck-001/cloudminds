package com.cloudminds.cdc.transform

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.types.Row

import scala.collection.JavaConversions._
import java.util

class ConvertRowToAvroRecordMapFunction(schemaStr:String) extends RichMapFunction[Row,GenericRecord]{
  var schema: Schema = _
  override def open(parameters: Configuration): Unit = {
    schema = new Schema.Parser().parse(schemaStr)
  }
  override def map(row: Row): GenericRecord = {
    val fields: util.List[Schema.Field] = schema.getFields

    val record = new GenericData.Record(schema)
    for (f <- fields){
      val o: Object = row.getField(f.name())
      record.put(f.name(),o)
    }
    record
  }
}
