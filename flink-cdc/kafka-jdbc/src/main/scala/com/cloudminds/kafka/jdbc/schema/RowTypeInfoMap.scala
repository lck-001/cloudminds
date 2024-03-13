package com.cloudminds.kafka.jdbc.schema

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.typeutils.Types

object RowTypeInfoMap {
  def getRowType(schemaType:String):TypeInformation[_]= {
    schemaType.toLowerCase match {
      case "string" => Types.STRING
      case "int" => Types.INT
      case "float" => Types.FLOAT
      case "double" => Types.DOUBLE
      case "bytes" => Types.BYTE
      case "long" => Types.LONG
      case "boolean" => Types.BOOLEAN
      case _ => Types.STRING
    }
  }
}
