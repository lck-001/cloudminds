package com.cloudminds.cdc.utils

import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.types.DataType

object DataTypeSchema {
  def getDataType(schemaType:String,data:Object):Object ={
    if (data != null){
      schemaType.toLowerCase match {
        /*case "string" => DataTypes.STRING()
        case "int" => DataTypes.INT()
        case "float" => DataTypes.FLOAT()
        case "double" => DataTypes.DOUBLE()
        case "bytes" => DataTypes.BYTES()
        case "long" => DataTypes.BIGINT()
        case "boolean" => DataTypes.BOOLEAN()
        case _ => DataTypes.STRING()*/
        case "string" => Option(data.toString).get
        case "int" => Option(data.toString.toInt).get.asInstanceOf[java.lang.Integer]
        case "float" => Option(data.toString.toFloat).get.asInstanceOf[java.lang.Float]
        case "double" =>Option(data.toString.toDouble).get.asInstanceOf[java.lang.Double]
//        case "bytes" => Option(data.toString.toByte).get.asInstanceOf[java.lang.Byte]
        case "long" => Option(data.toString.toLong).get.asInstanceOf[java.lang.Long]
        case "boolean" => Option(data.toString.toBoolean).get.asInstanceOf[java.lang.Boolean]
        case "bigdecimal" => Option(data.toString).get
        case _ => Option(data.toString).get
      }
    }else{
      null
    }
  }
}
