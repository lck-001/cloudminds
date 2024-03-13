package com.cloudminds.kafka.jdbc.schema

import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.types.DataType

object DataTypeMap {
  def getDataType(schemaType:String):DataType= {
    schemaType.toLowerCase match {
      case "string" => DataTypes.STRING()
      case "int" => DataTypes.INT()
      case "float" => DataTypes.FLOAT()
      case "double" => DataTypes.DOUBLE()
      case "bytes" => DataTypes.BYTES()
      case "long" => DataTypes.BIGINT()
      case "boolean" => DataTypes.BOOLEAN()
      case _ => DataTypes.STRING()
    }
  }
}
