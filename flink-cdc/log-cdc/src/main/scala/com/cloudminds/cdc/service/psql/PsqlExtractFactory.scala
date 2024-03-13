package com.cloudminds.cdc.service.psql

import com.alibaba.fastjson.{JSON, JSONObject}
import com.cloudminds.cdc.model.CommonSource
import com.cloudminds.cdc.service.ExtractFactory
import com.cloudminds.cdc.utils.DataTypeSchema
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}

import scala.collection.JavaConversions._
import java.util

class PsqlExtractFactory(schemaMap:util.HashMap[String,String]) extends ExtractFactory {
  override def getGenericRecord(common: CommonSource): GenericRecord = {
    val schemaStr: String = schemaMap.get(common.dbTable)
    assert(null != schemaStr && schemaStr.nonEmpty,"no schema defined !!!")
    val json: JSONObject = JSON.parseObject(common.data)
    val record = new GenericData.Record(new Schema.Parser().parse(schemaStr))
    var dataType: String = "string"
    for (f <- record.getSchema.getFields){
      val originalValue: Object = json.get(f.name())
      if (f.schema().getType.getName.equalsIgnoreCase("union")){
        dataType = f.schema().getTypes.get(0).getName
      }else if(originalValue.isInstanceOf[java.math.BigDecimal]){
        dataType = "BigDecimal"
      }else{
        dataType = f.schema().getType.getName
      }
      val value: Object = DataTypeSchema.getDataType(dataType, originalValue)
      record.put(f.name(),value)
    }
    record
  }
}
